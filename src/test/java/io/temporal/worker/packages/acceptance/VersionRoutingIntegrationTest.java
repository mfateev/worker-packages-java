package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.temporal.api.enums.v1.EventType;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.common.interceptors.Header;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.packages.runtime.PackageClientInterceptor;
import io.temporal.worker.packages.runtime.PackageHeaders;
import io.temporal.worker.packages.runtime.WorkerPackagePlugin;
import io.temporal.worker.packages.tooling.PackagePackager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/** Clients route by workflow type and queue; versions span independently isolated archives. */
public class VersionRoutingIntegrationTest {
  @Rule public TemporaryFolder temporary = new TemporaryFolder();
  @Rule public Timeout timeout = Timeout.seconds(120);

  @Test
  public void plainClientsUseCurrentVersionAndExplicitClientsOverrideItAcrossSplitArchives()
      throws Exception {
    Path temp = temporary.getRoot().toPath();
    Path packages = Files.createDirectories(temp.resolve("packages"));
    split(temp.resolve("fixtures"), packages, 2);
    split(temp.resolve("fixtures"), packages, 12);
    String queue = "version-routing-" + UUID.randomUUID();
    Consumer<WorkerPackagePlugin.Builder> current = builder -> builder.setCurrentVersion("12");
    Consumer<WorkerPackagePlugin.Builder> changed = builder -> builder.setCurrentVersion("2");
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient plain = WorkflowClient.newInstance(env.getWorkflowServiceStubs());
      WorkflowClient explicit =
          WorkflowClient.newInstance(
              env.getWorkflowServiceStubs(),
              WorkflowClientOptions.newBuilder()
                  .setPlugins(WorkerPackagePlugin.newBuilder().setClientVersion("2").build())
                  .build());
      WorkflowStub running =
          workflow(plain, queue, "default-" + UUID.randomUUID(), "OrderWorkflow");
      try (Host host = host(plain, packages, temp.resolve("first"), queue, current)) {
        host.start();
        assertTrue(host.plugin.toString().contains("currentVersion=12"));
        running.start(input("Current"), true, false);
        await(
            "default version is pinned",
            Duration.ofSeconds(20),
            () -> running.query("state", String.class).equals("v12:Current"));
        WorkflowStub overridden =
            workflow(explicit, queue, "explicit-" + UUID.randomUUID(), "OrderWorkflow");
        overridden.start(input("Explicit"), false, false);
        assertTrue(
            overridden
                .getResult(20, TimeUnit.SECONDS, String.class)
                .startsWith("workflow-v2|rules-v2:"));
        assertHeaders(plain.fetchHistory(overridden.getExecution().getWorkflowId()), "2", true);
        assertTrue(
            plain
                .fetchHistory(running.getExecution().getWorkflowId())
                .getEvents()
                .get(0)
                .getWorkflowExecutionStartedEventAttributes()
                .getHeader()
                .getFieldsMap()
                .isEmpty());
      }
      try (Host restarted = host(plain, packages, temp.resolve("second"), queue, changed)) {
        restarted.start();
        running.signal("proceed", input("Current"));
        assertTrue(
            running
                .getResult(20, TimeUnit.SECONDS, String.class)
                .startsWith("workflow-v12|rules-v12:"));
        assertHeaders(plain.fetchHistory(running.getExecution().getWorkflowId()), "12", false);
        WorkflowStub fresh =
            workflow(plain, queue, "new-current-" + UUID.randomUUID(), "OrderWorkflow");
        fresh.start(input("New current"), false, false);
        assertTrue(
            fresh
                .getResult(20, TimeUnit.SECONDS, String.class)
                .startsWith("workflow-v2|rules-v2:"));
      }
    }
  }

  @Test
  public void unavailableExplicitVersionFailsBeforePinningAndNeverInvokesDefaultSelector()
      throws Exception {
    Path temp = temporary.getRoot().toPath();
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(temp.resolve("fixtures"), 2), packages.resolve("orders.twp"));
    java.util.concurrent.atomic.AtomicInteger calls =
        new java.util.concurrent.atomic.AtomicInteger();
    Consumer<WorkerPackagePlugin.Builder> options =
        builder ->
            builder.setVersionSelector(
                context -> {
                  calls.incrementAndGet();
                  return "2";
                });
    String queue = "explicit-unavailable-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient explicit =
          WorkflowClient.newInstance(
              env.getWorkflowServiceStubs(),
              WorkflowClientOptions.newBuilder()
                  .setPlugins(WorkerPackagePlugin.newBuilder().setClientVersion("missing").build())
                  .build());
      WorkflowStub run = workflow(explicit, queue, "missing-" + UUID.randomUUID(), "OrderWorkflow");
      try (Host host = host(explicit, packages, temp.resolve("cache"), queue, options)) {
        host.start();
        run.start(input("Missing"), false, false);
        try {
          await(
              "explicit missing version fails task",
              Duration.ofSeconds(15),
              () ->
                  explicit.fetchHistory(run.getExecution().getWorkflowId()).getEvents().stream()
                      .anyMatch(
                          event ->
                              event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED));
          assertEquals(0, calls.get());
          for (io.temporal.api.history.v1.HistoryEvent event :
              explicit.fetchHistory(run.getExecution().getWorkflowId()).getEvents()) {
            assertNotEquals(EventType.EVENT_TYPE_MARKER_RECORDED, event.getEventType());
            assertNotEquals(EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, event.getEventType());
          }
        } finally {
          run.terminate("Explicit version test complete");
        }
      }
    }
  }

  @Test
  public void sdkPropagatesPackagePluginFromClientToWorkerWithoutExplicitFactoryRegistration()
      throws Exception {
    Path temp = temporary.getRoot().toPath();
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(temp.resolve("fixtures"), 2), packages.resolve("orders.twp"));
    String queue = "client-plugin-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkerPackagePlugin plugin =
          WorkerPackagePlugin.newBuilder()
              .setPackagesDirectory(packages)
              .setCacheDirectory(temp.resolve("cache"))
              .build();
      WorkflowClient client =
          WorkflowClient.newInstance(
              env.getWorkflowServiceStubs(),
              WorkflowClientOptions.newBuilder().setPlugins(plugin).build());
      assertTrue(
          java.util.Arrays.stream(client.getOptions().getInterceptors())
              .anyMatch(interceptor -> interceptor instanceof PackageClientInterceptor));
      io.temporal.worker.WorkerFactory factory =
          io.temporal.worker.WorkerFactory.newInstance(client);
      factory.newWorker(queue + "-second");
      try (Host host = new Host(plugin, factory, factory.newWorker(queue))) {
        host.start();
        WorkflowStub run = workflow(client, queue, "plugin-" + UUID.randomUUID(), "OrderWorkflow");
        run.start(input("Plugin"), false, false);
        assertTrue(
            run.getResult(20, TimeUnit.SECONDS, String.class).startsWith("workflow-v2|rules-v2:"));
        WorkflowStub second =
            workflow(client, queue + "-second", "second-" + UUID.randomUUID(), "OrderWorkflow");
        second.start(input("Second queue"), false, false);
        assertTrue(
            second
                .getResult(20, TimeUnit.SECONDS, String.class)
                .startsWith("workflow-v2|rules-v2:"));
        assertFalse(
            client
                .fetchHistory(run.getExecution().getWorkflowId())
                .getEvents()
                .get(0)
                .getWorkflowExecutionStartedEventAttributes()
                .getHeader()
                .containsFields(PackageHeaders.KEY));
      }
    }
  }

  private static void split(Path fixtures, Path packages, int version) throws Exception {
    FixtureCompiler.build(fixtures, version);
    Path input = fixtures.resolve("input-orders-" + version);
    Path manifest = input.resolve("workflow-package.json");
    ObjectMapper json = new ObjectMapper();
    ObjectNode original = (ObjectNode) json.readTree(manifest.toFile());
    ObjectNode workflows = original.deepCopy();
    workflows.put("packageId", "workflows");
    workflows.putArray("activities");
    Files.write(manifest, json.writeValueAsBytes(workflows));
    new PackagePackager().pack(manifest, input, packages.resolve("workflows-" + version + ".twp"));
    ObjectNode activities = original.deepCopy();
    activities.put("packageId", "activities");
    activities.putArray("workflows");
    Files.write(manifest, json.writeValueAsBytes(activities));
    new PackagePackager().pack(manifest, input, packages.resolve("activities-" + version + ".twp"));
  }

  private static void assertHeaders(
      WorkflowExecutionHistory history, String version, boolean explicit) {
    Header start =
        new Header(
            history.getEvents().get(0).getWorkflowExecutionStartedEventAttributes().getHeader());
    assertNull(PackageHeaders.workflowPackageId(start));
    assertEquals(explicit ? version : null, PackageHeaders.workflowVersion(start));
    int activities = 0;
    for (io.temporal.api.history.v1.HistoryEvent event : history.getEvents()) {
      if (event.getEventType() != EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) continue;
      activities++;
      io.temporal.worker.packages.api.PackageRef ref =
          PackageHeaders.activityRef(
              new Header(event.getActivityTaskScheduledEventAttributes().getHeader()));
      assertEquals(version, ref.packageVersion());
      assertEquals("activities", ref.packageId());
    }
    assertEquals(1, activities);
  }
}
