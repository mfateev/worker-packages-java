# Greeting example: evolve an activity API

Each version has one workflow, an activity interface, and their implementations. There are no DTOs, helper libraries, or framework dependencies beyond the provided Temporal SDK.

| | Version 1 | Version 2 |
| --- | --- | --- |
| Workflow | `GreetingWorkflow.run()` | Same interface |
| Original activity | `greet(String name)` | `greet(String greeting, String name)` |
| Added activity | — | `getName()` |
| Workflow body after waiting | `return activities.greet("World");` | `return activities.greet("Hi", name);` |
| Result | `Hello, World!` | `Hi, multi-versioned worker!` |

Both versions deliberately use the same Java package and class names. The activity type remains `Greet` even though its Java signature changes. `GetName` is exported only by version 2.

Version 1 waits for `proceed()` before calling `greet("World")`. Version 2 first calls `getName()`, waits for the same signal, then calls `greet("Hi", name)`. The demo observes committed workflow history to confirm readiness before restarting or signalling; the samples need no query method.

Run from the plugin repository root:

```bash
./scripts/demo.sh
```

The script starts its own local Temporal test service and:

1. Installs only version 1 and starts workflow A, which waits.
2. Adds version 2 and replaces the worker JVM against the same service.
3. Starts workflow B and signals both workflows.
4. Verifies A returns `Hello, World!` using the old activity signature, while B returns `Hi, multi-versioned worker!` using the changed signature and new activity.

Each sample applies the `io.temporal.worker-packages` Gradle plugin. Its `workerPackage` task packages the application JAR, runtime dependencies, and manifest into a `.twp`, and runs automatically as part of `assemble` and `build`. Like Gradle's `war` plugin, packaging belongs to the application build.

Version 1 declares its exports in [build.gradle](greeting-v1/build.gradle):

```groovy
plugins { id 'io.temporal.worker-packages' }
version = '1'

workerPackage {
    packageId = 'greetings'
    workflows = ['GreetingWorkflow': 'example.greeting.GreetingWorkflowImpl']
    activities = ['Greet': 'example.greeting.GreetingActivitiesImpl']
}
```

Version 2 adds `GetName` to its activities map and sets `version = '2'`. The package version defaults to the project's version as a string. Git SHAs and semantic versions are supported; set `packageVersion` explicitly to override the project version. Version identifiers have no implicit deployment ordering.

For an application whose package version should be its Git commit, configure its build with:

```groovy
version = providers.exec {
    commandLine 'git', 'rev-parse', 'HEAD'
}.standardOutput.asText.get().trim()
```

The resulting manifest and archive filename contain the SHA. Each installed archive must declare a distinct version; the two greeting variants keep `1` and `2` so they remain distinct when built from this same checkout.

Build either sample independently from the plugin repository root:

```bash
./gradlew -p examples/greeting-v1 build
./gradlew -p examples/greeting-v2 build
```

These produce `examples/greeting-v1/build/libs/greetings-1.twp` and `examples/greeting-v2/build/libs/greetings-2.twp`. To collect both archives into `build/examples/` for the worker and demo:

```bash
./gradlew packageExamples
```

Each archive contains a manifest and application JAR under `app/`. Dependencies declared with `implementation` or `runtimeOnly` are bundled under `lib/`; `compileOnly` dependencies are excluded. The samples declare the host-provided Temporal SDK as `compileOnly`. See [version 1](greeting-v1/src/main/java/example/greeting/GreetingWorkflowImpl.java) and [version 2](greeting-v2/src/main/java/example/greeting/GreetingWorkflowImpl.java).

The plugin is supplied locally through `pluginManagement { includeBuild('../../build-logic') }` in each sample's settings. Its implementation reuses the production package validator and packager. Tests consume these Gradle-built archives.

The SDK dependency version is declared in the root `gradle.properties`; compilation uses the local sdk-java main checkout. Packages and workers do not embed or compare SDK revisions.

To build a compatible patch, set `workerPackage.patchesVersion` to the version being fixed and give the patch its own `packageVersion`. The output becomes `greetings-<target>-<patch>.twp`. The greeting v1/v2 examples intentionally change activity signatures and are separate versions, not replay-compatible patches of each other. See [patched packages](../README.md#patched-packages) for chain deployment and checksum behavior.

## Worker implementation

[PackageWorker.java](worker/src/main/java/example/worker/PackageWorker.java) is a standalone host application. It takes one argument: the directory containing `.twp` packages. It opens `WorkerPackagePlugin`, installs it through `WorkerFactoryOptions.setPlugins`, creates a worker, and starts polling. Like the official Java samples, `main` starts polling and leaves the worker running.

Build the application and packages once using JDK 21, from the plugin repository root:

```bash
./gradlew packageExamples :example-worker:installDist
```

Run it directly with Java 8 or later, without Gradle:

```bash
TEMPORAL_PACKAGE_VERSION=2 examples/worker/build/install/example-worker/bin/example-worker build/examples
```

The worker installs a fixed version-selector callback from `TEMPORAL_PACKAGE_VERSION`. Set it to `2` for these commands. Without it, selection succeeds only when one version is installed. An embedding host can instead supply a callback that reads deployment configuration or selects by workflow metadata. Startup prints the selection policy and workflow/activity types exported by every installed version. For these samples the output includes:

```text
WorkerPackagePlugin{taskQueue=workflow-packages, versionSelector=configured}
  version=1 package=greetings sha256=...
    workflows=[GreetingWorkflow]
    activities=[Greet]
  version=2 package=greetings sha256=...
    workflows=[GreetingWorkflow]
    activities=[GetName, Greet]
```

Version strings must be unique across the entire package directory. Two archives declaring the same version cause startup to fail, including archives with different package IDs.

The worker follows the official [HelloActivity](https://github.com/temporalio/samples-java/blob/main/core/src/main/java/io/temporal/samples/hello/HelloActivity.java) and [HelloSignal](https://github.com/temporalio/samples-java/blob/main/core/src/main/java/io/temporal/samples/hello/HelloSignal.java) initialization pattern:

```java
ClientConfigProfile profile = ClientConfigProfile.load();
WorkflowServiceStubs service =
    WorkflowServiceStubs.newServiceStubs(profile.toWorkflowServiceStubsOptions());
WorkflowClient client =
    WorkflowClient.newInstance(service, profile.toWorkflowClientOptions());
```

Connection settings come from the standard Temporal TOML profile and environment variables. Use `TEMPORAL_ADDRESS` for the service address and `TEMPORAL_NAMESPACE` for the namespace. Select a profile with `TEMPORAL_PROFILE`, or a configuration file with `TEMPORAL_CONFIG_FILE`. With no configuration, the SDK connects to the local service on port 7233 in namespace `default`.

For example:

```bash
TEMPORAL_ADDRESS=127.0.0.1:7233 TEMPORAL_NAMESPACE=default \
  TEMPORAL_PACKAGE_VERSION=2 examples/worker/build/install/example-worker/bin/example-worker build/examples
```

Task queue `workflow-packages` and cache `.package-cache` remain example-specific settings, overridden with `TEMPORAL_TASK_QUEUE` and `TEMPORAL_PACKAGE_CACHE_DIR`. The plugin supplies workflow/activity registration. The sample intentionally omits shutdown hooks and lifecycle management; when an embedding host calls `factory.shutdown()`, the plugin releases package loaders after actual termination. The greeting workflow/activity interfaces remain in separate files so both package versions can use identical class names.

Use the service and client commands below to run the greeting workflow, and stop the worker with Ctrl-C. Those CLI commands use explicit connection flags; point them at the same service and namespace as the worker's profile. To deploy elsewhere, copy the worker distribution's entire `bin/` and `lib/` directories plus the package archives.

## Run the container CLI worker directly

Build the standalone distribution and sample archives once using JDK 21, from the plugin repository root:

```bash
./gradlew packageExamples installDist
```

After that, running the worker requires Java 8 or later and no Gradle. All commands below run from the plugin repository root.

Use an existing Temporal service, or start the bundled local test service in its own terminal:

```bash
build/install/temporal-worker-packages/bin/temporal-worker-packages server --port 7233
```

Keep that service running. In another terminal, start the container worker:

```bash
build/install/temporal-worker-packages/bin/temporal-worker-packages worker \
  --packages build/examples \
  --package-version 2 \
  --cache .package-cache \
  --endpoint 127.0.0.1:7233 \
  --namespace default \
  --task-queue workflow-packages
```

In a third terminal, start and release a sample workflow:

```bash
build/install/temporal-worker-packages/bin/temporal-worker-packages start \
  --package-id greetings --type GreetingWorkflow \
  --workflow-id greeting-direct-1 --input '[]' \
  --endpoint 127.0.0.1:7233 --namespace default --task-queue workflow-packages
build/install/temporal-worker-packages/bin/temporal-worker-packages signal \
  --workflow-id greeting-direct-1 --name proceed \
  --endpoint 127.0.0.1:7233 --namespace default
build/install/temporal-worker-packages/bin/temporal-worker-packages result \
  --workflow-id greeting-direct-1 --timeout-seconds 60 \
  --endpoint 127.0.0.1:7233 --namespace default
```

With both archives installed and `--package-version 2`, new workflows select version 2 and return `"Hi, multi-versioned worker!"`. Use a fresh workflow ID for another run. To try version 1, use `--package-version 1` (or a directory containing only `greetings-1.twp` without a version flag); its result is `"Hello, World!"`. Restart the worker after changing its package directory contents. Stop the worker with Ctrl-C.

For another machine, copy the entire `build/install/temporal-worker-packages/` directory (both `bin/` and `lib/`) and the `.twp` archives. Run `bin/temporal-worker-packages worker` from that distribution, adjusting package/cache paths and service connection flags. The distribution includes the host SDK libraries; no SDK checkout or Gradle installation is needed at runtime. The bundled test service is ephemeral, so use a persistent Temporal service when histories must survive service restarts.

The larger isolation and payload test cases are kept in `src/test/fixtures/`.
