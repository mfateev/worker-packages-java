package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;
import static org.junit.Assert.*;

import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.interceptors.Header;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.worker.packages.runtime.PackageHeaders;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

public class GreetingExampleTest {
  @Rule public Timeout timeout = Timeout.seconds(90);
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  Path temp;

  @Before
  public void initializeTemporaryFolder() {
    temp = temporaryFolder.getRoot().toPath();
  }

  @Test
  public void originalRunKeepsV1WhileV2ChangesTheSignatureAndAddsAnActivity() throws Exception {
    Path examples = temp.resolve("examples");
    Path original = ExamplePackages.copy(examples, 1);
    Path newer = ExamplePackages.copy(examples, 2);
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(original, packages.resolve("greetings-1.twp"));
    String queue = "greeting-example-" + UUID.randomUUID();
    try (io.temporal.testing.TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      io.temporal.client.WorkflowClient client = client(env.getWorkflowServiceStubs(), "greetings");
      io.temporal.client.WorkflowStub a =
          workflow(client, queue, "greeting-A-" + UUID.randomUUID(), "GreetingWorkflow");
      try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host first =
          host(client, packages, temp.resolve("cache-a"), queue)) {
        first.start();
        a.start();
        awaitGreetingWaiting(client, a, 1);
      }
      Files.copy(newer, packages.resolve("greetings-2.twp"));
      WorkflowExecutionHistory oldHistory;
      WorkflowExecutionHistory newHistory;
      try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host second =
          host(client, packages, temp.resolve("cache-b"), queue)) {
        second.start();
        io.temporal.client.WorkflowStub b =
            workflow(client, queue, "greeting-B-" + UUID.randomUUID(), "GreetingWorkflow");
        b.start();
        awaitGreetingWaiting(client, b, 2);
        a.signal("proceed");
        b.signal("proceed");
        assertEquals("Hello, World!", result(client, a, 20, String.class));
        assertEquals("Hi, multi-versioned worker!", result(client, b, 20, String.class));
        oldHistory = client.fetchHistory(a.getExecution().getWorkflowId());
        newHistory = client.fetchHistory(b.getExecution().getWorkflowId());
        assertHistory(
            oldHistory,
            second.plugin.runtime().catalog().require("greetings", 1).ref(),
            java.util.Arrays.asList("Greet"));
        assertHistory(
            newHistory,
            second.plugin.runtime().catalog().require("greetings", 2).ref(),
            java.util.Arrays.asList("GetName", "Greet"));
      }
      Path savedOld = temp.resolve("greeting-v1-history.json");
      Path savedNew = temp.resolve("greeting-v2-history.json");
      TestSupport.writeString(savedOld, oldHistory.toJson(true));
      TestSupport.writeString(savedNew, newHistory.toJson(true));
      try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host replay =
          host(client, packages, temp.resolve("cache-replay"), queue)) {
        replay.worker.replayWorkflowExecution(
            WorkflowExecutionHistory.fromJson(TestSupport.readString(savedOld)));
        replay.worker.replayWorkflowExecution(
            WorkflowExecutionHistory.fromJson(TestSupport.readString(savedNew)));
      }
    }
  }

  private static void assertHistory(
      WorkflowExecutionHistory history, PackageRef expected, List<String> activities) {
    io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes started =
        history.getEvents().get(0).getWorkflowExecutionStartedEventAttributes();
    assertEquals("GreetingWorkflow", started.getWorkflowType().getName());
    assertEquals(0, started.getInput().getPayloadsCount());
    assertEquals("greetings", PackageHeaders.workflowPackageId(new Header(started.getHeader())));
    List<HistoryEvent> scheduled =
        history.getEvents().stream()
            .filter(event -> event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(
        activities,
        scheduled.stream()
            .map(
                event ->
                    event.getActivityTaskScheduledEventAttributes().getActivityType().getName())
            .collect(java.util.stream.Collectors.toList()));
    for (HistoryEvent event : scheduled) {
      io.temporal.api.history.v1.ActivityTaskScheduledEventAttributes activity =
          event.getActivityTaskScheduledEventAttributes();
      assertEquals(expected, PackageHeaders.activityRef(new Header(activity.getHeader())));
      if (activity.getActivityType().getName().equals("GetName")) {
        assertEquals(0, activity.getInput().getPayloadsCount());
      }
      if (activity.getActivityType().getName().equals("Greet")) {
        assertEquals(
            "the original logical activity type keeps each version's method signature",
            expected.packageVersion().equals("1") ? 1 : 2,
            activity.getInput().getPayloadsCount());
        if (expected.packageVersion().equals("1")) {
          assertEquals(
              "World",
              DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                  0, Optional.of(activity.getInput()), String.class, String.class));
        } else {
          assertEquals(
              "Hi",
              DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                  0, Optional.of(activity.getInput()), String.class, String.class));
          assertEquals(
              "multi-versioned worker",
              DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                  1, Optional.of(activity.getInput()), String.class, String.class));
        }
      }
    }
    long pinMarker = Long.MAX_VALUE;
    for (HistoryEvent event : history.getEvents()) {
      if (event.getEventType() != EventType.EVENT_TYPE_MARKER_RECORDED) continue;
      io.temporal.api.history.v1.MarkerRecordedEventAttributes marker =
          event.getMarkerRecordedEventAttributes();
      assertNotEquals("Version", marker.getMarkerName());
      if (marker.getMarkerName().equals("SideEffect")) {
        pinMarker = event.getEventId();
        String[] pin =
            DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                0, Optional.of(marker.getDetailsOrThrow("data")), String[].class, String[].class);
        assertArrayEquals(new String[] {expected.packageVersion(), expected.sha256()}, pin);
      }
    }
    assertTrue(
        "the durable package pin precedes application activity commands",
        pinMarker < scheduled.get(0).getEventId());
  }
}
