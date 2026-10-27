package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;
import static org.junit.Assert.*;

import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.interceptors.Header;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.worker.packages.runtime.PackageHeaders;
import io.temporal.worker.packages.runtime.WorkerPackagePlugin;
import io.temporal.worker.packages.tooling.PackageCatalog;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/** Exercises a host policy that treats package versions as opaque strings. */
public class VersionSelectorIntegrationTest {
  private static final String OLD = "adf0fc802850d5a35fc5ea34de8c9afbfddb6d6f";
  private static final String NEW = "release-candidate";
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public Timeout timeout = Timeout.seconds(120);

  @Test
  public void hostSelectsOpaqueVersionsOncePerRunAndReplayKeepsTheAtomicPin() throws Exception {
    Path temp = temporaryFolder.getRoot().toPath();
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(
        FixtureCompiler.build(temp.resolve("fixtures"), 2, "orders", OLD),
        packages.resolve("old.twp"));
    Files.copy(
        FixtureCompiler.build(temp.resolve("fixtures"), 12, "orders", NEW),
        packages.resolve("new.twp"));
    PackageCatalog catalog = PackageCatalog.scan(packages);
    PackageRef oldRef = catalog.require("orders", OLD).ref();
    PackageRef newRef = catalog.require("orders", NEW).ref();
    AtomicReference<String> selected = new AtomicReference<>(OLD);
    AtomicInteger selections = new AtomicInteger();
    Consumer<WorkerPackagePlugin.Builder> options =
        builder ->
            builder.setVersionSelector(
                context -> {
                  selections.incrementAndGet();
                  assertNull(context.getPackageId());
                  assertEquals("OrderWorkflow", context.getWorkflowType());
                  assertTrue(context.getWorkflowId().startsWith("opaque-"));
                  assertFalse(context.getRunId().isEmpty());
                  assertEquals(
                      new HashSet<>(Arrays.asList(OLD, NEW)),
                      new HashSet<>(context.getAvailableVersions()));
                  return selected.get();
                });
    String queue = "opaque-selector-" + UUID.randomUUID();
    AtomicInteger replaySelections = new AtomicInteger();
    Consumer<WorkerPackagePlugin.Builder> replayOptions =
        builder ->
            builder.setVersionSelector(
                context -> {
                  replaySelections.incrementAndGet();
                  throw new IllegalStateException("A durable pin must bypass the host selector");
                });
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      WorkflowStub a = workflow(client, queue, "opaque-old-" + UUID.randomUUID(), "OrderWorkflow");
      WorkflowStub b = workflow(client, queue, "opaque-new-" + UUID.randomUUID(), "OrderWorkflow");
      try (Host first = host(client, packages, temp.resolve("cache-first"), queue, options)) {
        first.start();
        a.start(input("Old"), true, false);
        await(
            "old opaque version pinned",
            Duration.ofSeconds(20),
            () -> a.query("state", String.class).equals("v2:Old"));
        assertEquals(1, selections.get());
        selected.set(NEW);
        b.start(input("New"), true, false);
        await(
            "new opaque version pinned",
            Duration.ofSeconds(20),
            () -> b.query("state", String.class).equals("v12:New"));
        assertEquals(2, selections.get());
        assertEquals("v2:Old", a.query("state", String.class));
        assertEquals(2, selections.get());
      }
      WorkflowExecutionHistory oldHistory;
      WorkflowExecutionHistory newHistory;
      try (Host restarted =
          host(client, packages, temp.resolve("cache-restarted"), queue, replayOptions)) {
        restarted.start();
        a.signal("proceed", input("Old"));
        b.signal("proceed", input("New"));
        assertTrue(
            a.getResult(25, TimeUnit.SECONDS, String.class).startsWith("workflow-v2|rules-v2:"));
        assertTrue(
            b.getResult(25, TimeUnit.SECONDS, String.class).startsWith("workflow-v12|rules-v12:"));
        oldHistory = client.fetchHistory(a.getExecution().getWorkflowId());
        newHistory = client.fetchHistory(b.getExecution().getWorkflowId());
        assertPinAndActivityHeader(oldHistory, oldRef);
        assertPinAndActivityHeader(newHistory, newRef);
        assertEquals(0, replaySelections.get());
      }
      Path oldSaved = temp.resolve("old-history.json");
      Path newSaved = temp.resolve("new-history.json");
      TestSupport.writeString(oldSaved, oldHistory.toJson(true));
      TestSupport.writeString(newSaved, newHistory.toJson(true));
      try (Host replay =
          host(client, packages, temp.resolve("cache-replay"), queue, replayOptions)) {
        replay.worker.replayWorkflowExecution(
            WorkflowExecutionHistory.fromJson(TestSupport.readString(oldSaved)));
        replay.worker.replayWorkflowExecution(
            WorkflowExecutionHistory.fromJson(TestSupport.readString(newSaved)));
      }
      assertEquals(2, selections.get());
      assertEquals(0, replaySelections.get());
    }
  }

  @Test
  public void nullAndUnavailableSelectionsFailBeforePinOrApplicationAndRetryCanRecover()
      throws Exception {
    Path temp = temporaryFolder.getRoot().toPath();
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(
        FixtureCompiler.build(temp.resolve("fixtures"), 2, "orders", OLD),
        packages.resolve("old.twp"));
    AtomicReference<String> selected = new AtomicReference<>();
    AtomicInteger selections = new AtomicInteger();
    AtomicInteger unavailableSelections = new AtomicInteger();
    Consumer<WorkerPackagePlugin.Builder> options =
        builder ->
            builder.setVersionSelector(
                context -> {
                  selections.incrementAndGet();
                  String version = selected.get();
                  if ("not-installed".equals(version)) unavailableSelections.incrementAndGet();
                  return version;
                });
    String queue = "invalid-selector-" + UUID.randomUUID();
    System.clearProperty("fixture.workflow.invoked.v2");
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      WorkflowStub run =
          client.newUntypedWorkflowStub(
              "OrderWorkflow",
              WorkflowOptions.newBuilder()
                  .setWorkflowId("invalid-" + UUID.randomUUID())
                  .setTaskQueue(queue)
                  .setWorkflowTaskTimeout(Duration.ofSeconds(2))
                  .setWorkflowExecutionTimeout(Duration.ofMinutes(2))
                  .build());
      try (Host host = host(client, packages, temp.resolve("cache"), queue, options)) {
        host.start();
        run.start(input("Recovered"), false, false);
        await(
            "null selection fails a workflow task",
            Duration.ofSeconds(20),
            () -> failedTasks(client, run) >= 1);
        assertNoPinOrApplication(client, run);
        selected.set("not-installed");
        // Repeated task failures may be suppressed in history; a second callback proves retry.
        await(
            "unavailable selection is retried without committing a pin",
            Duration.ofSeconds(20),
            () -> unavailableSelections.get() >= 2);
        assertNoPinOrApplication(client, run);
        selected.set(OLD);
        assertTrue(
            run.getResult(30, TimeUnit.SECONDS, String.class).startsWith("workflow-v2|rules-v2:"));
        assertTrue(selections.get() >= 3);
        assertPinAndActivityHeader(
            client.fetchHistory(run.getExecution().getWorkflowId()),
            host.plugin.runtime().catalog().require("orders", OLD).ref());
      }
    } finally {
      System.clearProperty("fixture.workflow.invoked.v2");
    }
  }

  private static long failedTasks(WorkflowClient client, WorkflowStub run) {
    return client.fetchHistory(run.getExecution().getWorkflowId()).getEvents().stream()
        .filter(event -> event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED)
        .count();
  }

  private static void assertNoPinOrApplication(WorkflowClient client, WorkflowStub run) {
    assertNull(System.getProperty("fixture.workflow.invoked.v2"));
    for (HistoryEvent event : client.fetchHistory(run.getExecution().getWorkflowId()).getEvents()) {
      assertNotEquals(EventType.EVENT_TYPE_MARKER_RECORDED, event.getEventType());
      assertNotEquals(EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, event.getEventType());
      assertNotEquals(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, event.getEventType());
    }
  }

  private static void assertPinAndActivityHeader(
      WorkflowExecutionHistory history, PackageRef expected) {
    long pin = Long.MAX_VALUE;
    int activities = 0;
    int pinCount = 0;
    for (HistoryEvent event : history.getEvents()) {
      if (event.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED) {
        io.temporal.api.history.v1.MarkerRecordedEventAttributes marker =
            event.getMarkerRecordedEventAttributes();
        if (marker.getMarkerName().equals("SideEffect")) {
          pin = event.getEventId();
          pinCount++;
          String[] value =
              DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                  0, Optional.of(marker.getDetailsOrThrow("data")), String[].class, String[].class);
          assertArrayEquals(
              new String[] {expected.packageVersion(), expected.sha256(), expected.packageId()},
              value);
        } else if (marker.getMarkerName().equals("Version")) {
          String change =
              DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                  0, Optional.of(marker.getDetailsOrThrow("changeId")), String.class, String.class);
          assertEquals("application.patch", change);
        }
      }
      if (event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) {
        activities++;
        assertTrue(pin < event.getEventId());
        assertEquals(
            "ChargeCard",
            event.getActivityTaskScheduledEventAttributes().getActivityType().getName());
        assertEquals(
            expected,
            PackageHeaders.activityRef(
                new Header(event.getActivityTaskScheduledEventAttributes().getHeader())));
      }
    }
    assertEquals(1, pinCount);
    assertEquals(1, activities);
  }
}
