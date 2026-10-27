package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.api.enums.v1.EventType;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.testserver.TestServer;
import io.temporal.worker.packages.tooling.PackageCatalog;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

public class CrossProcessRetryTest {
  @Rule public Timeout timeout = Timeout.seconds(150);
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  Path temp;

  @Before
  public void initializeTemporaryFolder() {
    temp = temporaryFolder.getRoot().toPath();
  }

  @Test
  public void retryInFreshJvmUsesOriginalHeaderAndCodeWhileHigherVersionIsCurrent()
      throws Exception {
    Path fixtures = temp.resolve("fixtures");
    Path original = FixtureCompiler.build(fixtures, 2);
    Path newer = FixtureCompiler.build(fixtures, 12);
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(original, packages.resolve("original.twp"));
    int port = ProcessHost.availablePort();
    String target = "127.0.0.1:" + port;
    String queue = "cross-process-" + UUID.randomUUID();
    try (TestServer.PortBoundTestServer server = TestServer.createPortBoundServer(port, true)) {
      WorkflowServiceStubs service =
          WorkflowServiceStubs.newServiceStubs(TestServices.options().setTarget(target).build());
      try {
        io.temporal.client.WorkflowClient client = client(service);
        WorkflowStub run = workflow(client, queue, "retry-" + UUID.randomUUID(), "OrderWorkflow");
        try (io.temporal.worker.packages.acceptance.ProcessHost first =
            new ProcessHost(
                target, packages, temp.resolve("cache-a"), queue, temp.resolve("worker-a.log"))) {
          run.start(input("retry:cross-process"), false, false);
          await(
              "retryable failure of first activity attempt",
              Duration.ofSeconds(20),
              () ->
                  run.describe().getRawDescription().getPendingActivitiesList().stream()
                      .anyMatch(
                          a ->
                              a.hasLastFailure()
                                  && a.getLastFailure()
                                      .getApplicationFailureInfo()
                                      .getType()
                                      .equals("FixtureRetry")));
        }
        Files.copy(newer, packages.resolve("new-current.twp"));
        try (io.temporal.worker.packages.acceptance.ProcessHost second =
            new ProcessHost(
                target, packages, temp.resolve("cache-b"), queue, temp.resolve("worker-b.log"))) {
          assertEquals(
              "workflow-v2|rules-v2:retry:cross-process:3|resource=rules-v2",
              result(client, run, 40, String.class));
          WorkflowStub fresh = workflow(client, queue, "new-" + UUID.randomUUID(), "OrderWorkflow");
          fresh.start(input("New"), false, false);
          assertTrue(
              fresh
                  .getResult(20, TimeUnit.SECONDS, String.class)
                  .contains("workflow-v12|rules-v12:"));
        }
        String expectedDigest = PackageCatalog.scan(packages).require("orders", 2).ref().sha256();
        io.temporal.api.history.v1.ActivityTaskScheduledEventAttributes scheduled =
            client.fetchHistory(run.getExecution().getWorkflowId()).getEvents().stream()
                .filter(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected matching history event"))
                .getActivityTaskScheduledEventAttributes();
        assertEquals("ChargeCard", scheduled.getActivityType().getName());
        com.fasterxml.jackson.databind.JsonNode header =
            new ObjectMapper()
                .readTree(
                    scheduled
                        .getHeader()
                        .getFieldsOrThrow(FixtureCompiler.HEADER)
                        .getData()
                        .toByteArray());
        assertTrue(header.get("packageVersion").isTextual());
        assertEquals("2", header.get("packageVersion").textValue());
        assertEquals(expectedDigest, header.get("sha256").textValue());
      } finally {
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }
}
