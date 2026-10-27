package io.temporal.worker.packages.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowTargetOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.testserver.TestServer;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.worker.packages.runtime.WorkerPackagePlugin;
import io.temporal.worker.packages.tooling.PackageCatalog;
import io.temporal.worker.packages.tooling.PackagePackager;
import io.temporal.worker.packages.tooling.PackageValidator;
import io.temporal.worker.packages.tooling.ValidatedPackage;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/** Host command line. Application arguments are JSON; package routing uses Temporal headers. */
public final class Main {
  private static final ObjectMapper JSON = new ObjectMapper();

  private Main() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
      System.out.println(
          "Usage: temporal-worker-packages COMMAND [--flag value | --flag=value]\n"
              + "  list       --packages DIR\n"
              + "  validate   --archive FILE\n"
              + "  pack       --manifest FILE --input-root DIR --output FILE\n"
              + "  worker     --packages DIR --cache DIR [--package-version VERSION] --endpoint HOST:PORT --namespace NS --task-queue Q\n"
              + "  start      [--package-version VERSION] --type TYPE --workflow-id ID --input '[arguments]'\n"
              + "  signal     --workflow-id ID --name SIGNAL --input '[arguments]'\n"
              + "  query      --workflow-id ID --name QUERY --input '[arguments]'\n"
              + "  update     --workflow-id ID --name UPDATE --input '[arguments]'\n"
              + "  result     --workflow-id ID [--timeout-seconds 60]\n"
              + "  server     [--port 7233] (local test server; no persistent storage)\n"
              + "Client commands also accept --endpoint, --namespace, --task-queue and optional --run-id.\n"
              + "Flags override TEMPORAL_PACKAGES, TEMPORAL_PACKAGE_CACHE_DIR, TEMPORAL_ENDPOINT,\n"
              + "TEMPORAL_NAMESPACE, TEMPORAL_TASK_QUEUE, and TEMPORAL_PACKAGE_VERSION.\n");
      return;
    }
    Options options = Options.parse(args);
    switch (args[0]) {
      case "list":
        {
          PackageCatalog catalog =
              PackageCatalog.scan(options.path("packages", "TEMPORAL_PACKAGES", "packages"));
          System.out.println("PACKAGE\tVERSION\tPATCHES VERSION\tARCHIVE CHECKSUM\tARCHIVE");
          for (ValidatedPackage entry : catalog.entries()) {
            PackageRef ref = entry.ref();
            System.out.printf(
                "%s\t%s\t%s\t%s\t%s%n",
                ref.packageId(),
                ref.packageVersion(),
                entry.manifest().patchesVersion() == null ? "-" : entry.manifest().patchesVersion(),
                ref.sha256(),
                entry.archive());
          }
          break;
        }
      case "validate":
        {
          ValidatedPackage entry =
              new PackageValidator().validate(Paths.get(options.required("archive")));
          System.out.printf(
              "Valid: %s version %s archive checksum %s%n",
              entry.ref().packageId(), entry.ref().packageVersion(), entry.ref().sha256());
          if (entry.manifest().patchesVersion() != null)
            System.out.println("Patches version " + entry.manifest().patchesVersion());
          break;
        }
      case "pack":
        {
          Path output = Paths.get(options.required("output"));
          new PackagePackager()
              .pack(
                  Paths.get(options.required("manifest")),
                  Paths.get(options.required("input-root")),
                  output);
          ValidatedPackage entry = new PackageValidator().validate(output);
          System.out.printf("%s archive checksum %s%n", output, entry.ref().sha256());
          break;
        }
      case "worker":
        worker(options);
        break;
      case "server":
        server(options);
        break;
      case "start":
      case "signal":
      case "query":
      case "update":
      case "result":
        client(args[0], options);
        break;
      default:
        throw new IllegalArgumentException("Unknown command: " + args[0] + "; use help");
    }
  }

  private static void worker(Options options) throws Exception {
    String queue = options.get("task-queue", "TEMPORAL_TASK_QUEUE", "workflow-packages");
    WorkerPackagePlugin.Builder pluginBuilder =
        WorkerPackagePlugin.newBuilder()
            .setPackagesDirectory(options.path("packages", "TEMPORAL_PACKAGES", "packages"))
            .setCacheDirectory(
                options.path("cache", "TEMPORAL_PACKAGE_CACHE_DIR", ".package-cache"));
    String version = options.get("package-version", "TEMPORAL_PACKAGE_VERSION", null);
    if (version != null) {
      PackageRef.validateVersion(version);
      pluginBuilder.setCurrentVersion(version);
    }
    WorkerPackagePlugin plugin = pluginBuilder.build();
    WorkflowServiceStubs service = null;
    WorkerFactory factory = null;
    Thread hook = null;
    CountDownLatch stopped = new CountDownLatch(1);
    try {
      service = service(options);
      WorkflowClient client = WorkflowClient.newInstance(service, clientOptions(options, null));
      factory =
          WorkerFactory.newInstance(
              client, WorkerFactoryOptions.newBuilder().setPlugins(plugin).build());
      factory.newWorker(queue);
      WorkerFactory ownedFactory = factory;
      hook =
          new Thread(
              () -> {
                try {
                  drain(ownedFactory, plugin);
                } finally {
                  stopped.countDown();
                }
              },
              "worker-package-shutdown");
      Runtime.getRuntime().addShutdownHook(hook);
      factory.start();
      System.out.println("Worker polling " + queue);
      stopped.await();
    } finally {
      if (hook != null) {
        try {
          Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException ignored) {
          /* JVM shutdown already owns the hook. */
        }
      }
      try {
        if (factory != null) drain(factory, plugin);
        else plugin.close();
      } finally {
        if (service != null) {
          service.shutdown();
          service.awaitTermination(30, TimeUnit.SECONDS);
        }
      }
    }
  }

  private static void drain(WorkerFactory factory, WorkerPackagePlugin plugin) {
    factory.shutdown();
    if (!awaitFactoryTermination(factory, 60, TimeUnit.SECONDS)) {
      factory.shutdownNow();
      if (!awaitFactoryTermination(factory, 30, TimeUnit.SECONDS)) {
        String reason =
            Thread.currentThread().isInterrupted()
                ? "Interrupted while waiting for worker termination"
                : "Worker did not terminate";
        throw new IllegalStateException(reason + "; package loaders remain open");
      }
    }
    try {
      plugin.closeAfterTermination(factory);
    } catch (java.io.IOException failure) {
      throw new java.io.UncheckedIOException(failure);
    }
  }

  private static boolean awaitFactoryTermination(
      WorkerFactory factory, long timeout, TimeUnit unit) {
    long deadline = System.nanoTime() + unit.toNanos(timeout);
    long remaining = deadline - System.nanoTime();
    if (!factory.isTerminated() && !Thread.currentThread().isInterrupted() && remaining > 0) {
      factory.awaitTermination(remaining, TimeUnit.NANOSECONDS);
    }
    // SDK executor termination can precede completion of its asynchronous shutdown chain,
    // especially for workers whose registration failed before polling ever started.
    while (!factory.isTerminated()) {
      if (Thread.currentThread().isInterrupted()) return false;
      remaining = deadline - System.nanoTime();
      if (remaining <= 0) return false;
      LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(10)));
    }
    return true;
  }

  private static void server(Options options) throws Exception {
    int port = options.positiveInt("port", "7233");
    TestServer.PortBoundTestServer server = TestServer.createPortBoundServer(port);
    AtomicBoolean closed = new AtomicBoolean();
    Runnable close =
        () -> {
          if (closed.compareAndSet(false, true)) server.close();
        };
    try {
      CountDownLatch stopped = new CountDownLatch(1);
      Thread hook =
          new Thread(
              () -> {
                try {
                  close.run();
                } finally {
                  stopped.countDown();
                }
              },
              "test-server-shutdown");
      Runtime.getRuntime().addShutdownHook(hook);
      try {
        System.out.println("Local test server listening on port " + port);
        stopped.await();
      } finally {
        try {
          Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException ignored) {
        }
      }
    } finally {
      close.run();
    }
  }

  private static void client(String command, Options options) throws Exception {
    String version =
        command.equals("start")
            ? options.get("package-version", "TEMPORAL_PACKAGE_VERSION", null)
            : null;
    WorkflowServiceStubs service = service(options);
    try {
      WorkflowClient client = WorkflowClient.newInstance(service, clientOptions(options, version));
      String workflowId = options.required("workflow-id");
      Object[] arguments = JSON.readValue(options.get("input", null, "[]"), Object[].class);
      if (command.equals("start")) {
        WorkflowStub stub =
            client.newUntypedWorkflowStub(
                options.required("type"),
                WorkflowOptions.newBuilder()
                    .setTaskQueue(
                        options.get("task-queue", "TEMPORAL_TASK_QUEUE", "workflow-packages"))
                    .setWorkflowId(workflowId)
                    .build());
        WorkflowExecution execution = stub.start(arguments);
        Map<String, String> executionResult = new LinkedHashMap<>();
        executionResult.put("workflowId", execution.getWorkflowId());
        executionResult.put("runId", execution.getRunId());
        System.out.println(JSON.writeValueAsString(executionResult));
      } else {
        String runId = options.get("run-id", null, null);
        WorkflowStub stub =
            client.newUntypedWorkflowStub(
                WorkflowTargetOptions.newBuilder()
                    .setWorkflowId(workflowId)
                    .setRunId(runId)
                    .build());
        Object result;
        switch (command) {
          case "signal":
            stub.signal(options.required("name"), arguments);
            result = "signal accepted";
            break;
          case "query":
            result = stub.query(options.required("name"), Object.class, arguments);
            break;
          case "update":
            result = stub.update(options.required("name"), Object.class, arguments);
            break;
          case "result":
            result =
                stub.getResult(
                    options.positiveInt("timeout-seconds", "60"), TimeUnit.SECONDS, Object.class);
            break;
          default:
            throw new AssertionError(command);
        }
        System.out.println(JSON.writeValueAsString(result));
      }
    } finally {
      service.shutdown();
      service.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  private static WorkflowServiceStubs service(Options options) {
    return WorkflowServiceStubs.newServiceStubs(
        WorkflowServiceStubsOptions.newBuilder()
            .setTarget(options.get("endpoint", "TEMPORAL_ENDPOINT", "127.0.0.1:7233"))
            .setRpcTimeout(Duration.ofSeconds(10))
            .build());
  }

  private static WorkflowClientOptions clientOptions(Options options, String version)
      throws java.io.IOException {
    WorkflowClientOptions.Builder builder =
        WorkflowClientOptions.newBuilder()
            .setNamespace(options.get("namespace", "TEMPORAL_NAMESPACE", "default"));
    WorkerPackagePlugin.Builder plugin = WorkerPackagePlugin.newBuilder();
    if (version != null) plugin.setClientVersion(version);
    builder.setPlugins(plugin.build());
    return builder.build();
  }

  private static final class Options {
    private final Map<String, String> flags;

    private Options(Map<String, String> flags) {
      this.flags = Collections.unmodifiableMap(new LinkedHashMap<>(flags));
    }

    private static final Set<String> KNOWN =
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    "packages",
                    "cache",
                    "endpoint",
                    "namespace",
                    "task-queue",
                    "archive",
                    "manifest",
                    "input-root",
                    "output",
                    "package-version",
                    "type",
                    "workflow-id",
                    "input",
                    "name",
                    "run-id",
                    "timeout-seconds",
                    "port")));

    static Options parse(String[] args) {
      Map<String, String> flags = new LinkedHashMap<>();
      for (int index = 1; index < args.length; index++) {
        String argument = args[index];
        if (!argument.startsWith("--"))
          throw new IllegalArgumentException("Expected --flag: " + argument);
        int equals = argument.indexOf('=');
        String key = argument.substring(2, equals < 0 ? argument.length() : equals);
        if (!KNOWN.contains(key)) throw new IllegalArgumentException("Unknown flag --" + key);
        String value;
        if (equals >= 0) value = argument.substring(equals + 1);
        else {
          if (++index >= args.length || args[index].startsWith("--"))
            throw new IllegalArgumentException("Missing value for --" + key);
          value = args[index];
        }
        if (flags.putIfAbsent(key, value) != null)
          throw new IllegalArgumentException("Repeated flag --" + key);
      }
      return new Options(flags);
    }

    String get(String flag, String environment, String fallback) {
      String value = flags.get(flag);
      if (value == null && environment != null) value = System.getenv(environment);
      return value == null || value.codePoints().allMatch(Character::isWhitespace)
          ? fallback
          : value;
    }

    String required(String flag) {
      String value = get(flag, null, null);
      if (value == null) throw new IllegalArgumentException("Required --" + flag);
      return value;
    }

    Path path(String flag, String environment, String fallback) {
      return Paths.get(get(flag, environment, fallback));
    }

    int positiveInt(String flag, String fallback) {
      int value = Integer.parseInt(get(flag, null, fallback));
      if (value < 1) throw new IllegalArgumentException("--" + flag + " must be positive");
      return value;
    }
  }
}
