package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;
import static org.junit.Assert.*;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.DynamicActivity;
import io.temporal.api.enums.v1.EventType;
import io.temporal.client.WorkflowClient;
import io.temporal.common.SimplePlugin;
import io.temporal.common.converter.EncodedValues;
import io.temporal.worker.*;
import io.temporal.worker.packages.runtime.PackageRuntimeException;
import io.temporal.worker.packages.runtime.WorkerPackagePlugin;
import io.temporal.workflow.*;
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

public class PluginLifecycleIntegrationTest {
  @Rule public Timeout timeout = Timeout.seconds(90);
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  Path temp;

  @Before
  public void initializeTemporaryFolder() {
    temp = temporaryFolder.getRoot().toPath();
  }

  private Path packages() throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(temp.resolve("fixtures"), 2), packages.resolve("orders.twp"));
    return packages;
  }

  @Test
  public void existingPreferredVersionProviderOnManagedQueueIsPreserved() throws Exception {
    Path packages = packages();
    String queue = "provider-preserved-" + UUID.randomUUID();
    java.util.concurrent.atomic.AtomicInteger calls =
        new java.util.concurrent.atomic.AtomicInteger();
    try (io.temporal.testing.TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs(), "orders");
      WorkerPackagePlugin plugin = WorkerPackagePlugin.open(packages, temp.resolve("cache"), queue);
      WorkerFactory factory =
          WorkerFactory.newInstance(
              client, WorkerFactoryOptions.newBuilder().setPlugins(plugin).build());
      try {
        factory.newWorker(
            queue,
            WorkerOptions.newBuilder()
                .setPreferredVersionProvider(
                    input -> {
                      calls.incrementAndGet();
                      return VersionPreference.of(3);
                    })
                .build());
        factory.start();
        io.temporal.client.WorkflowStub run =
            workflow(client, queue, "provider-" + UUID.randomUUID(), "OrderWorkflow");
        run.start(input("Preserved"), false, false);
        assertTrue(
            run.getResult(20, TimeUnit.SECONDS, String.class).startsWith("workflow-v2|rules-v2:"));
        assertTrue("SDK application patch provider is preserved", calls.get() > 0);
      } finally {
        terminate(factory, plugin);
      }
    }
  }

  @Test
  public void existingDynamicWorkflowRegistrationProducesActionableConflict() throws Exception {
    assertDynamicConflict(true);
  }

  @Test
  public void existingDynamicActivityRegistrationProducesActionableConflict() throws Exception {
    assertDynamicConflict(false);
  }

  private void assertDynamicConflict(boolean workflow) throws Exception {
    Path packages = packages();
    String queue = "dynamic-conflict-" + UUID.randomUUID();
    try (io.temporal.testing.TestWorkflowEnvironment env = TestServices.environment()) {
      WorkflowClient client = client(env.getWorkflowServiceStubs(), "orders");
      WorkerPackagePlugin plugin = WorkerPackagePlugin.open(packages, temp.resolve("cache"), queue);
      SimplePlugin preceding =
          new SimplePlugin("acceptance.conflicting-dynamic") {
            @Override
            public void initializeWorker(String name, Worker worker) {
              if (workflow) worker.registerWorkflowImplementationTypes(OtherDynamicWorkflow.class);
              else worker.registerActivitiesImplementations((DynamicActivity) arguments -> "other");
            }
          };
      io.temporal.worker.WorkerFactory factory =
          WorkerFactory.newInstance(
              client, WorkerFactoryOptions.newBuilder().setPlugins(preceding, plugin).build());
      try {
        PackageRuntimeException error =
            assertThrows(PackageRuntimeException.class, () -> factory.newWorker(queue));
        assertTrue(
            error.getMessage().contains("exclusive dynamic workflow/activity registrations"));
      } finally {
        terminate(factory, plugin);
      }
    }
  }

  @Test
  public void unrelatedWorkerAndOrdinaryWorkflowActivityAndProviderPassThrough() throws Exception {
    Path packages = packages();
    String managedQueue = "managed-" + UUID.randomUUID();
    String ordinaryQueue = managedQueue + "-ordinary";
    try (io.temporal.testing.TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient packagedClient = client(env.getWorkflowServiceStubs(), "orders");
      WorkflowClient plainClient = WorkflowClient.newInstance(env.getWorkflowServiceStubs());
      try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host host =
          host(packagedClient, packages, temp.resolve("cache"), managedQueue)) {
        Worker ordinary =
            host.factory.newWorker(
                ordinaryQueue,
                WorkerOptions.newBuilder()
                    .setPreferredVersionProvider(input -> VersionPreference.of(7))
                    .build());
        ordinary.registerWorkflowImplementationTypes(OrdinaryWorkflowImpl.class);
        ordinary.registerActivitiesImplementations(new OrdinaryActivityImpl());
        host.start();
        io.temporal.client.WorkflowStub plain =
            workflow(
                plainClient, ordinaryQueue, "ordinary-" + UUID.randomUUID(), "OrdinaryWorkflow");
        plain.start("hello");
        assertEquals("7:ordinary:hello", plain.getResult(20, TimeUnit.SECONDS, String.class));
        io.temporal.common.WorkflowExecutionHistory history =
            plainClient.fetchHistory(plain.getExecution().getWorkflowId());
        assertFalse(
            history
                .getEvents()
                .get(0)
                .getWorkflowExecutionStartedEventAttributes()
                .getHeader()
                .containsFields(FixtureCompiler.HEADER));
        io.temporal.api.history.v1.ActivityTaskScheduledEventAttributes activity =
            history.getEvents().stream()
                .filter(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected matching history event"))
                .getActivityTaskScheduledEventAttributes();
        assertFalse(activity.getHeader().containsFields(FixtureCompiler.HEADER));
        io.temporal.client.WorkflowStub packaged =
            workflow(
                packagedClient, managedQueue, "packaged-" + UUID.randomUUID(), "OrderWorkflow");
        packaged.start(input("Managed"), false, false);
        assertTrue(
            packaged
                .getResult(20, TimeUnit.SECONDS, String.class)
                .startsWith("workflow-v2|rules-v2:"));
      }
    }
  }

  @Test
  public void sdkInvokesPluginHooksInOrderAndForwardsEachLifecycleOnce() throws Exception {
    Path packages = packages();
    String queue = "hook-order-" + UUID.randomUUID();
    java.util.concurrent.CopyOnWriteArrayList<String> events =
        new java.util.concurrent.CopyOnWriteArrayList<String>();
    SimplePlugin observer =
        new SimplePlugin("acceptance.lifecycle-observer") {
          @Override
          public void configureWorkerFactory(WorkerFactoryOptions.Builder builder) {
            events.add("configure-factory");
          }

          @Override
          public void configureWorker(String name, WorkerOptions.Builder builder) {
            events.add("configure-worker");
          }

          @Override
          public void initializeWorker(String name, Worker worker) {
            events.add("initialize-worker");
          }

          @Override
          public void startWorkerFactory(
              WorkerFactory factory, java.util.function.Consumer<WorkerFactory> next) {
            events.add("start-factory");
            next.accept(factory);
          }

          @Override
          public void startWorker(
              String name, Worker worker, java.util.function.BiConsumer<String, Worker> next) {
            events.add("start-worker");
            next.accept(name, worker);
          }

          @Override
          public void shutdownWorkerFactory(
              WorkerFactory factory, java.util.function.Consumer<WorkerFactory> next) {
            events.add("shutdown-factory");
            next.accept(factory);
          }

          @Override
          public void shutdownWorker(
              String name, Worker worker, java.util.function.BiConsumer<String, Worker> next) {
            events.add("shutdown-worker");
            next.accept(name, worker);
          }
        };
    try (io.temporal.testing.TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs(), "orders");
      WorkerPackagePlugin plugin = WorkerPackagePlugin.open(packages, temp.resolve("cache"), queue);
      io.temporal.worker.WorkerFactory factory =
          WorkerFactory.newInstance(
              client, WorkerFactoryOptions.newBuilder().setPlugins(observer, plugin).build());
      try {
        factory.newWorker(queue);
        assertEquals(
            java.util.Arrays.asList("configure-factory", "configure-worker", "initialize-worker"),
            events);
        factory.start();
        io.temporal.client.WorkflowStub run =
            workflow(client, queue, "hooks-" + UUID.randomUUID(), "OrderWorkflow");
        run.start(input("Hooks"), false, false);
        assertTrue(result(client, run, 20, String.class).contains("workflow-v2"));
        factory.shutdown();
        factory.awaitTermination(20, TimeUnit.SECONDS);
        assertTrue(factory.isTerminated());
        assertNoWorkerThreads(queue);
        assertEquals(
            java.util.Arrays.asList(
                "configure-factory",
                "configure-worker",
                "initialize-worker",
                "start-factory",
                "start-worker",
                "shutdown-factory",
                "shutdown-worker"),
            events);
      } finally {
        terminate(factory, plugin);
      }
    }
  }

  @Test
  public void packageLoadersRemainUsableUntilRunningActivitiesHaveDrained() throws Exception {
    Path packages = packages();
    String queue = "drain-" + UUID.randomUUID();
    Path release = temp.resolve("release-activity");
    try (io.temporal.testing.TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs(), "orders");
      io.temporal.client.WorkflowStub run =
          workflow(client, queue, "drain-run-" + UUID.randomUUID(), "DrainWorkflow");
      try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host host =
          host(client, packages, temp.resolve("cache-draining"), queue)) {
        try {
          host.start();
          run.start(release.toAbsolutePath().toString());
          await(
              "activity executing before graceful shutdown",
              Duration.ofSeconds(20),
              () ->
                  run.describe().getRawDescription().getPendingActivitiesList().stream()
                      .anyMatch(a -> a.hasHeartbeatDetails()));
          host.factory.shutdown();
          assertFalse(host.factory.isTerminated());
          assertThrows(
              PackageRuntimeException.class, () -> host.plugin.closeAfterTermination(host.factory));
          TestSupport.writeString(release, "release");
          host.factory.awaitTermination(20, TimeUnit.SECONDS);
          assertTrue("activity drains before package loader release", host.factory.isTerminated());
          io.temporal.api.history.v1.HistoryEvent completed =
              client.fetchHistory(run.getExecution().getWorkflowId()).getEvents().stream()
                  .filter(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED)
                  .findFirst()
                  .orElseThrow(() -> new AssertionError("Expected matching history event"));
          String value =
              io.temporal.common.converter.DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                  0,
                  java.util.Optional.of(
                      completed.getActivityTaskCompletedEventAttributes().getResult()),
                  String.class,
                  String.class);
          assertEquals(
              "resource lookup after shutdown request still uses an open loader",
              "drained-rules-v2",
              value);
          await(
              "plugin closes package loaders automatically after activity draining",
              Duration.ofSeconds(5),
              () -> {
                try {
                  host.plugin.runtime().catalog();
                  return false;
                } catch (PackageRuntimeException closed) {
                  return true;
                }
              });
        } finally {
          TestSupport.writeString(release, "release-on-failure");
        }
      }
      try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host restarted =
          host(client, packages, temp.resolve("cache-after-drain"), queue)) {
        restarted.start();
        assertEquals("drained-rules-v2", result(client, run, 20, String.class));
      }
    } finally {
      TestSupport.writeString(release, "release-on-failure");
    }
  }

  private static void terminate(WorkerFactory factory, WorkerPackagePlugin plugin)
      throws Exception {
    factory.shutdownNow();
    factory.awaitTermination(20, TimeUnit.SECONDS);
    await(
        "failed-start asynchronous shutdown callbacks",
        Duration.ofSeconds(5),
        factory::isTerminated);
    assertTrue("failed-start factory must terminate before loader release", factory.isTerminated());
    plugin.closeAfterTermination(factory);
  }

  public static class OtherDynamicWorkflow implements DynamicWorkflow {
    public OtherDynamicWorkflow() {}

    @Override
    public Object execute(EncodedValues arguments) {
      return "other";
    }
  }

  @WorkflowInterface
  public interface OrdinaryWorkflow {
    @WorkflowMethod(name = "OrdinaryWorkflow")
    String run(String value);
  }

  @ActivityInterface
  public interface OrdinaryActivity {
    @ActivityMethod(name = "OrdinaryActivity")
    String run(String value);
  }

  public static class OrdinaryActivityImpl implements OrdinaryActivity {
    public OrdinaryActivityImpl() {}

    public String run(String value) {
      return "ordinary:" + value;
    }
  }

  public static class OrdinaryWorkflowImpl implements OrdinaryWorkflow {
    public OrdinaryWorkflowImpl() {}

    public String run(String value) {
      int version = Workflow.getVersion("ordinary.change", 1, 10);
      String activity =
          Workflow.newActivityStub(
                  OrdinaryActivity.class,
                  ActivityOptions.newBuilder()
                      .setStartToCloseTimeout(Duration.ofSeconds(10))
                      .build())
              .run(value);
      return version + ":" + activity;
    }
  }
}
