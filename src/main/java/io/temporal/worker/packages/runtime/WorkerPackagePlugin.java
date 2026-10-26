package io.temporal.worker.packages.runtime;

import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.WorkerOptions;
import io.temporal.worker.WorkerPlugin;
import io.temporal.worker.packages.tooling.PackageCatalog;
import io.temporal.worker.packages.tooling.PackageManifest;
import io.temporal.worker.packages.tooling.ValidatedPackage;
import io.temporal.workflow.WorkflowInfo;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Worker-only plugin. Each instance owns one prepared runtime and one worker factory. */
public final class WorkerPackagePlugin implements WorkerPlugin, AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(WorkerPackagePlugin.class);
  private final PreparedPackageRuntime runtime;
  private final String taskQueue;
  private final String diagnostics;
  private final Options options;
  private boolean configured;
  private WorkerFactory owner;
  private int activeReplays;
  private boolean cleanupScheduled;

  public static WorkerPackagePlugin open(Path packages, Path cache, String taskQueue)
      throws IOException {
    return open(packages, cache, taskQueue, Options.newBuilder().build());
  }

  public static WorkerPackagePlugin open(
      Path packages, Path cache, String taskQueue, Options options) throws IOException {
    Objects.requireNonNull(options, "options");
    validateTaskQueue(taskQueue);
    return new WorkerPackagePlugin(
        PreparedPackageRuntime.open(packages, cache), taskQueue, options);
  }

  public WorkerPackagePlugin(PreparedPackageRuntime runtime, String taskQueue) {
    this(runtime, taskQueue, Options.newBuilder().build());
  }

  public WorkerPackagePlugin(PreparedPackageRuntime runtime, String taskQueue, Options options) {
    this.runtime = Objects.requireNonNull(runtime, "runtime");
    this.taskQueue = validateTaskQueue(taskQueue);
    this.options = Objects.requireNonNull(options, "options");
    runtime.checkOpen();
    diagnostics = describe(runtime.catalog(), taskQueue, options);
  }

  private static String validateTaskQueue(String taskQueue) {
    Objects.requireNonNull(taskQueue, "taskQueue");
    if (taskQueue.codePoints().allMatch(Character::isWhitespace))
      throw new IllegalArgumentException("Task queue must not be blank");
    return taskQueue;
  }

  /** Selects an installed version when a new workflow records its durable package pin. */
  @FunctionalInterface
  public interface VersionSelector {
    /** May run again if a workflow task fails before committing the pin. */
    String selectVersion(SelectionContext context);
  }

  /** Immutable workflow metadata and installed versions supplied to a version selector. */
  public static final class SelectionContext {
    private final String packageId;
    private final WorkflowInfo workflowInfo;
    private final List<String> availableVersions;

    SelectionContext(String packageId, WorkflowInfo workflowInfo, PackageCatalog catalog) {
      this.packageId = packageId;
      this.workflowInfo = workflowInfo;
      this.availableVersions = catalog.selectionVersions();
    }

    public String getPackageId() {
      return packageId;
    }

    public String getWorkflowType() {
      return workflowInfo.getWorkflowType();
    }

    public String getWorkflowId() {
      return workflowInfo.getWorkflowId();
    }

    public String getRunId() {
      return workflowInfo.getRunId();
    }

    public WorkflowInfo getWorkflowInfo() {
      return workflowInfo;
    }

    /** Returns installed versions and patch target aliases, without deployment ordering. */
    public List<String> getAvailableVersions() {
      return availableVersions;
    }
  }

  /** Immutable host policy for selecting the implementation of each new workflow. */
  public static final class Options {
    private final VersionSelector versionSelector;
    private final boolean customSelector;

    private Options(Builder builder) {
      versionSelector = builder.versionSelector;
      customSelector = builder.customSelector;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public VersionSelector getVersionSelector() {
      return versionSelector;
    }

    public static final class Builder {
      private VersionSelector versionSelector =
          context -> {
            if (context.getAvailableVersions().size() != 1)
              throw new PackageRuntimeException(
                  "Multiple package versions installed; configure a version selector in plugin options");
            return context.getAvailableVersions().get(0);
          };
      private boolean customSelector;

      private Builder() {}

      /**
       * Sets the host policy. It is called only while recording a new durable pin, never on replay.
       * The callback must support concurrent workflows and return an installed version for the
       * requested package. A task failure before commit may cause the callback to run again.
       */
      public Builder setVersionSelector(VersionSelector versionSelector) {
        this.versionSelector = Objects.requireNonNull(versionSelector, "versionSelector");
        customSelector = true;
        return this;
      }

      public Options build() {
        return new Options(this);
      }
    }
  }

  @Override
  public String toString() {
    return diagnostics;
  }

  private static String describe(PackageCatalog catalog, String taskQueue, Options options) {
    String current =
        !options.customSelector && catalog.selectionVersions().size() == 1
            ? catalog.selectionVersions().get(0)
            : null;
    StringBuilder output =
        new StringBuilder("WorkerPackagePlugin{taskQueue=")
            .append(taskQueue)
            .append(", versionSelector=")
            .append(options.customSelector ? "configured" : "singleInstalled");
    if (current != null) output.append(", currentVersion=").append(current);
    output.append('}');
    for (ValidatedPackage entry : catalog.entries()) {
      output.append("\n  version=").append(entry.ref().packageVersion());
      if (entry.ref().packageVersion().equals(current)) output.append(" (current)");
      if (entry.manifest().patchesVersion() != null)
        output.append(" patchesVersion=").append(entry.manifest().patchesVersion());
      output
          .append(" package=")
          .append(entry.ref().packageId())
          .append(" sha256=")
          .append(entry.ref().sha256())
          .append("\n    workflows=")
          .append(types(entry.manifest().workflows()))
          .append("\n    activities=")
          .append(types(entry.manifest().activities()));
    }
    return output.toString();
  }

  private static String types(List<PackageManifest.Entrypoint> entries) {
    return entries.stream()
        .map(PackageManifest.Entrypoint::type)
        .sorted()
        .collect(Collectors.joining(", ", "[", "]"));
  }

  public PreparedPackageRuntime runtime() {
    return runtime;
  }

  public String taskQueue() {
    return taskQueue;
  }

  @Override
  public String getName() {
    return "io.temporal.worker-packages";
  }

  @Override
  public synchronized void configureWorkerFactory(WorkerFactoryOptions.Builder builder) {
    runtime.checkOpen();
    if (configured)
      throw new PackageRuntimeException(
          "Create a separate package plugin/runtime for each worker factory");
    WorkerInterceptor[] previous = builder.build().getWorkerInterceptors();
    int count = previous == null ? 0 : previous.length;
    WorkerInterceptor[] combined =
        previous == null ? new WorkerInterceptor[1] : Arrays.copyOf(previous, count + 1);
    combined[count] =
        new PackageWorkerInterceptor(runtime, taskQueue, options.getVersionSelector());
    builder.setWorkerInterceptors(combined);
    configured = true;
  }

  @Override
  public void configureWorker(String queue, WorkerOptions.Builder builder) {
    if (taskQueue.equals(queue)) runtime.checkOpen();
  }

  @Override
  public void initializeWorker(String queue, Worker worker) {
    if (!taskQueue.equals(queue)) return;
    runtime.checkOpen();
    try {
      worker.registerWorkflowImplementationTypes(ContainerDynamicWorkflow.class);
      worker.registerActivitiesImplementations(new ContainerDynamicActivity(runtime));
    } catch (RuntimeException e) {
      throw new PackageRuntimeException(
          "Managed queue " + queue + " requires exclusive dynamic workflow/activity registrations",
          e);
    }
  }

  @Override
  public void startWorker(String queue, Worker worker, BiConsumer<String, Worker> next) {
    runtime.checkOpen();
    next.accept(queue, worker);
  }

  @Override
  public void startWorkerFactory(WorkerFactory factory, Consumer<WorkerFactory> next) {
    synchronized (this) {
      runtime.checkOpen();
      if (owner != null && owner != factory)
        throw new PackageRuntimeException("Package plugin belongs to another factory");
      owner = factory;
    }
    next.accept(factory);
  }

  private synchronized void scheduleCleanup(WorkerFactory factory) {
    if (owner != null && owner != factory)
      throw new PackageRuntimeException("Package plugin belongs to another factory");
    owner = factory;
    if (cleanupScheduled) return;
    Thread cleanup =
        new Thread(() -> cleanupAfterTermination(factory), "worker-package-cleanup-" + taskQueue);
    cleanup.setDaemon(true);
    cleanup.setContextClassLoader(WorkerPackagePlugin.class.getClassLoader());
    cleanupScheduled = true;
    try {
      cleanup.start();
    } catch (RuntimeException | Error failure) {
      cleanupScheduled = false;
      throw failure;
    }
  }

  private void cleanupAfterTermination(WorkerFactory factory) {
    // Shutdown hooks run under the factory monitor. Observe termination from another thread.
    while (!Thread.currentThread().isInterrupted()) {
      if (factory.isTerminated()) {
        synchronized (this) {
          if (activeReplays == 0) {
            try {
              runtime.close();
            } catch (IOException failure) {
              log.warn("Failed to close package loaders for task queue {}", taskQueue, failure);
            }
            return;
          }
        }
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }
    log.warn("Package cleanup interrupted for task queue {}; loaders remain open", taskQueue);
  }

  @Override
  public void shutdownWorker(String queue, Worker worker, BiConsumer<String, Worker> next) {
    next.accept(queue, worker);
  }

  @Override
  public void shutdownWorkerFactory(WorkerFactory factory, Consumer<WorkerFactory> next) {
    // TODO: Expose an SDK plugin awaitTermination callback that distinguishes actual termination
    // from timeout, so the plugin can close package loaders without host-owned cleanup or a
    // watcher.
    try {
      next.accept(factory);
    } finally {
      if (factory != null) scheduleCleanup(factory);
    }
  }

  @Override
  public void replayWorkflowExecution(
      Worker worker, WorkflowExecutionHistory history, ReplayCallback next) throws Exception {
    synchronized (this) {
      runtime.checkOpen();
      if (cleanupScheduled)
        throw new PackageRuntimeException(
            "Cannot start saved-history replay after factory shutdown");
      activeReplays++;
    }
    try {
      next.replay(worker, history);
    } finally {
      synchronized (this) {
        activeReplays--;
      }
    }
  }

  /**
   * Call only after shutdown and an actual isTerminated() check, including replay-only factories.
   */
  public void closeAfterTermination(WorkerFactory factory) throws IOException {
    Objects.requireNonNull(factory);
    if (!factory.isTerminated())
      throw new PackageRuntimeException(
          "Worker factory must terminate before closing package loaders");
    synchronized (this) {
      if (owner != null && owner != factory)
        throw new PackageRuntimeException("Package plugin belongs to another factory");
      if (activeReplays != 0)
        throw new PackageRuntimeException(
            "Wait for saved-history replay before closing package loaders");
      owner = factory;
      runtime.close();
    }
  }

  /**
   * Closes a never-started runtime, including failed attachment, or an actually terminated owner.
   */
  @Override
  public void close() throws IOException {
    WorkerFactory factory;
    synchronized (this) {
      if (activeReplays != 0)
        throw new PackageRuntimeException(
            "Wait for saved-history replay before closing package loaders");
      factory = owner;
      if (factory == null) {
        // Closing prevents this configured but unstarted factory from starting later.
        runtime.close();
        return;
      }
    }
    closeAfterTermination(factory);
  }
}
