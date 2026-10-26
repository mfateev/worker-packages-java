package io.temporal.worker.packages.acceptance;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.packages.runtime.PackageClientInterceptor;
import io.temporal.worker.packages.runtime.WorkerPackagePlugin;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

final class IntegrationSupport {
  private IntegrationSupport() {}

  static WorkflowClient client(WorkflowServiceStubs service, String packageId) {
    return WorkflowClient.newInstance(
        service,
        WorkflowClientOptions.newBuilder()
            .setNamespace("default")
            .setInterceptors(new PackageClientInterceptor(packageId))
            .build());
  }

  static WorkflowStub workflow(WorkflowClient client, String queue, String id, String type) {
    return client.newUntypedWorkflowStub(
        type,
        WorkflowOptions.newBuilder()
            .setWorkflowId(id)
            .setTaskQueue(queue)
            .setWorkflowExecutionTimeout(Duration.ofMinutes(2))
            .build());
  }

  static Map<String, String> input(String name) {
    return TestSupport.map("customer_name", name);
  }

  static void await(String evidence, Duration timeout, BooleanSupplier condition) {
    long deadline = System.nanoTime() + timeout.toNanos();
    Throwable last = null;
    while (System.nanoTime() < deadline) {
      try {
        if (condition.getAsBoolean()) return;
      } catch (RuntimeException e) {
        last = e;
      }
      LockSupport.parkNanos(Duration.ofMillis(25).toNanos());
      if (Thread.currentThread().isInterrupted())
        throw new IllegalStateException("Interrupted waiting for " + evidence);
    }
    throw new AssertionError("Timed out waiting for " + evidence, last);
  }

  static Host host(
      WorkflowClient client, Path packages, Path cache, String queue, WorkerInterceptor... existing)
      throws Exception {
    return host(client, packages, cache, queue, numericFixtureOptions(), existing);
  }

  // Numeric fixture labels are a test-host policy, not container version semantics.
  static WorkerPackagePlugin.Options numericFixtureOptions() {
    return WorkerPackagePlugin.Options.newBuilder()
        .setVersionSelector(
            context ->
                context.getAvailableVersions().stream()
                    .max(java.util.Comparator.comparingInt(Integer::parseInt))
                    .orElseThrow(() -> new IllegalStateException("No fixture versions installed")))
        .build();
  }

  static Host host(
      WorkflowClient client,
      Path packages,
      Path cache,
      String queue,
      WorkerPackagePlugin.Options options,
      WorkerInterceptor... existing)
      throws Exception {
    WorkerPackagePlugin plugin = WorkerPackagePlugin.open(packages, cache, queue, options);
    WorkerFactory factory = null;
    try {
      factory =
          WorkerFactory.newInstance(
              client,
              WorkerFactoryOptions.newBuilder()
                  .setWorkerInterceptors(existing)
                  .setPlugins(plugin)
                  .build());
      Worker worker = factory.newWorker(queue);
      return new Host(plugin, factory, worker);
    } catch (Throwable failure) {
      try {
        if (factory != null) {
          factory.shutdownNow();
          factory.awaitTermination(10, TimeUnit.SECONDS);
          WorkerFactory failedFactory = factory;
          await(
              "failed-start asynchronous shutdown callbacks",
              Duration.ofSeconds(5),
              failedFactory::isTerminated);
          if (!factory.isTerminated())
            throw new IllegalStateException(
                "Failed-start factory did not terminate; retaining loaders");
          plugin.closeAfterTermination(factory);
        } else {
          plugin.close();
        }
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  static <T> T result(WorkflowClient client, WorkflowStub run, long timeoutSeconds, Class<T> type)
      throws Exception {
    try {
      return run.getResult(timeoutSeconds, TimeUnit.SECONDS, type);
    } catch (java.util.concurrent.TimeoutException failure) {
      try {
        Path output =
            java.nio.file.Files.createDirectories(
                FixtureCompiler.root().resolve("build/diagnostics"));
        String id = run.getExecution().getWorkflowId().replaceAll("[^A-Za-z0-9_.-]", "_");
        TestSupport.writeString(
            output.resolve(id + "-history.json"),
            client.fetchHistory(run.getExecution().getWorkflowId()).toJson(true));
        TestSupport.writeString(
            output.resolve(id + "-description.txt"), run.describe().getRawDescription().toString());
        System.err.println("Timed out workflow evidence: " + output.resolve(id + "-history.json"));
      } catch (Exception diagnosticFailure) {
        failure.addSuppressed(diagnosticFailure);
      }
      throw failure;
    }
  }

  static void assertNoWorkerThreads(String queue) {
    await(
        "terminated worker threads for " + queue,
        Duration.ofSeconds(5),
        () ->
            Thread.getAllStackTraces().keySet().stream()
                .noneMatch(thread -> thread.isAlive() && thread.getName().contains(queue)));
  }

  static final class Host implements AutoCloseable {
    final WorkerPackagePlugin plugin;
    final WorkerFactory factory;
    final Worker worker;
    private boolean closed;

    Host(WorkerPackagePlugin plugin, WorkerFactory factory, Worker worker) {
      this.plugin = plugin;
      this.factory = factory;
      this.worker = worker;
    }

    void start() {
      factory.start();
    }

    @Override
    public void close() throws Exception {
      if (closed) return;
      // Replay-only factories have no polling work to drain.
      if (factory.isStarted()) factory.shutdown();
      else factory.shutdownNow();
      factory.awaitTermination(20, TimeUnit.SECONDS);
      if (!factory.isTerminated()) {
        factory.shutdownNow();
        factory.awaitTermination(10, TimeUnit.SECONDS);
      }
      if (!factory.isTerminated())
        throw new IllegalStateException("Worker did not terminate; loaders retained");
      plugin.closeAfterTermination(factory);
      assertNoWorkerThreads(worker.getTaskQueue());
      closed = true;
    }
  }
}
