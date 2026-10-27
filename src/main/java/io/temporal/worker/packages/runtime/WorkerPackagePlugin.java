package io.temporal.worker.packages.runtime;

import io.temporal.client.WorkflowClientOptions;
import io.temporal.common.SimplePlugin;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.common.interceptors.WorkflowClientInterceptor;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.WorkerOptions;
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

/** Client and worker plugin. Worker instances own one prepared runtime and one worker factory. */
public final class WorkerPackagePlugin extends SimplePlugin implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(WorkerPackagePlugin.class);
  private final PreparedPackageRuntime runtime;
  private final String diagnostics;
  private final VersionSelector versionSelector;
  private final String currentVersion;
  private final boolean customSelector;
  private final String clientVersion;
  private boolean configured;
  private WorkerFactory owner;
  private int activeReplays;
  private boolean cleanupScheduled;

  private WorkerPackagePlugin(Builder builder) throws IOException {
    super("io.temporal.worker-packages");
    if (builder.packagesDirectory == null
        && (builder.cacheDirectory != null || builder.workerPolicyConfigured))
      throw new IllegalArgumentException("Worker configuration requires a package directory");
    versionSelector = builder.versionSelector;
    currentVersion = builder.currentVersion;
    customSelector = builder.customSelector;
    clientVersion = builder.clientVersion;
    runtime =
        builder.packagesDirectory == null
            ? null
            : PreparedPackageRuntime.open(
                builder.packagesDirectory,
                builder.cacheDirectory == null
                    ? java.nio.file.Paths.get(".package-cache")
                    : builder.cacheDirectory);
    diagnostics =
        runtime == null
            ? "WorkerPackagePlugin{clientOnly=true, version="
                + (clientVersion == null ? "workerAssigned" : clientVersion)
                + "}"
            : describe(runtime.catalog(), currentVersion, customSelector);
  }

  public static Builder newBuilder() {
    return new Builder();
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

    /** Returns a legacy header hint, or null for ordinary/version-only client starts. */
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

  /** Unified client and worker configuration. An empty builder creates a client-only plugin. */
  public static final class Builder {
    private Path packagesDirectory;
    private Path cacheDirectory;
    private String clientVersion;
    private String currentVersion;
    private boolean customSelector;
    private boolean workerPolicyConfigured;
    private VersionSelector versionSelector =
        context -> {
          if (context.getAvailableVersions().size() != 1)
            throw new PackageRuntimeException(
                "Multiple package versions installed; configure a current version or version selector");
          return context.getAvailableVersions().get(0);
        };

    private Builder() {}

    /** Enables package loading on every worker in this plugin's factory. */
    public Builder setPackagesDirectory(Path packagesDirectory) {
      this.packagesDirectory = Objects.requireNonNull(packagesDirectory, "packagesDirectory");
      return this;
    }

    /** Sets the prepared package cache directory; defaults to .package-cache. */
    public Builder setCacheDirectory(Path cacheDirectory) {
      this.cacheDirectory = Objects.requireNonNull(cacheDirectory, "cacheDirectory");
      return this;
    }

    /** Requests a version on client workflow starts; unset by default for worker selection. */
    public Builder setClientVersion(String version) {
      clientVersion = io.temporal.worker.packages.api.PackageRef.validateVersion(version);
      return this;
    }

    /** Sets the worker's default version for starts without an explicit client version. */
    public Builder setCurrentVersion(String version) {
      currentVersion = io.temporal.worker.packages.api.PackageRef.validateVersion(version);
      versionSelector = context -> version;
      customSelector = false;
      workerPolicyConfigured = true;
      return this;
    }

    /**
     * Selects a default version when recording a new workflow's durable pin, never on replay. The
     * callback must support concurrent workflows. A task failure before commit may call it again.
     * The last current-version or selector setting wins.
     */
    public Builder setVersionSelector(VersionSelector selector) {
      versionSelector = Objects.requireNonNull(selector, "versionSelector");
      currentVersion = null;
      customSelector = true;
      workerPolicyConfigured = true;
      return this;
    }

    /** Builds an independent plugin, validating and preparing worker packages before polling. */
    public WorkerPackagePlugin build() throws IOException {
      return new WorkerPackagePlugin(this);
    }
  }

  @Override
  public String toString() {
    return diagnostics;
  }

  private static String describe(
      PackageCatalog catalog, String currentVersion, boolean customSelector) {
    String current =
        currentVersion != null
            ? currentVersion
            : !customSelector && catalog.selectionVersions().size() == 1
                ? catalog.selectionVersions().get(0)
                : null;
    StringBuilder output =
        new StringBuilder("WorkerPackagePlugin{versionSelector=")
            .append(
                customSelector
                    ? "configured"
                    : currentVersion != null ? "currentVersion" : "singleInstalled");
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
    if (runtime == null)
      throw new PackageRuntimeException("Client-only plugin has no package runtime");
    return runtime;
  }

  @Override
  public String getName() {
    return "io.temporal.worker-packages";
  }

  @Override
  public void configureWorkflowClient(WorkflowClientOptions.Builder builder) {
    WorkflowClientInterceptor[] previous = builder.build().getInterceptors();
    int count = previous == null ? 0 : previous.length;
    WorkflowClientInterceptor[] combined =
        previous == null ? new WorkflowClientInterceptor[1] : Arrays.copyOf(previous, count + 1);
    combined[count] =
        clientVersion == null
            ? new PackageClientInterceptor()
            : new PackageClientInterceptor(clientVersion);
    builder.setInterceptors(combined);
  }

  @Override
  public synchronized void configureWorkerFactory(WorkerFactoryOptions.Builder builder) {
    if (runtime == null) return;
    runtime.checkOpen();
    if (configured)
      throw new PackageRuntimeException(
          "Create a separate package plugin/runtime for each worker factory");
    WorkerInterceptor[] previous = builder.build().getWorkerInterceptors();
    int count = previous == null ? 0 : previous.length;
    WorkerInterceptor[] combined =
        previous == null ? new WorkerInterceptor[1] : Arrays.copyOf(previous, count + 1);
    combined[count] = new PackageWorkerInterceptor(runtime, versionSelector);
    builder.setWorkerInterceptors(combined);
    configured = true;
  }

  @Override
  public void configureWorker(String queue, WorkerOptions.Builder builder) {
    if (runtime != null) runtime.checkOpen();
  }

  @Override
  public void initializeWorker(String queue, Worker worker) {
    if (runtime == null) return;
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
    if (runtime != null) runtime.checkOpen();
    next.accept(queue, worker);
  }

  @Override
  public void startWorkerFactory(WorkerFactory factory, Consumer<WorkerFactory> next) {
    if (runtime == null) {
      next.accept(factory);
      return;
    }
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
    Thread cleanup = new Thread(() -> cleanupAfterTermination(factory), "worker-package-cleanup");
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
              log.warn("Failed to close package loaders", failure);
            }
            return;
          }
        }
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }
    log.warn("Package cleanup interrupted; loaders remain open");
  }

  @Override
  public void shutdownWorker(String queue, Worker worker, BiConsumer<String, Worker> next) {
    next.accept(queue, worker);
  }

  @Override
  public void shutdownWorkerFactory(WorkerFactory factory, Consumer<WorkerFactory> next) {
    if (runtime == null) {
      next.accept(factory);
      return;
    }
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
    if (runtime == null) {
      next.replay(worker, history);
      return;
    }
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
    if (runtime == null) return;
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
    if (runtime == null) return;
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
