# Temporal Worker Packages for Java

A Java 8-compatible worker plugin for loading independent versions of workflow/activity packages in one JVM. The root project publishes one library and supplies its CLI distribution. The included `build-logic/` build supplies the packaging Gradle plugin. This directory is an independent Git repository; the SDK checkout is its sibling.

Workflow and activity types keep their ordinary annotated names. Clients route using ordinary workflow types and task queues. An optional `container.package` header requests a version; otherwise the worker assigns its configured current version. A recorded side effect durably pins the version and workflow archive. Activities resolve independently by type at that version and carry their archive references in headers for retries on a fresh worker.

See [ACCEPTANCE_REPORT.md](ACCEPTANCE_REPORT.md) for the commands, complete acceptance mapping, demonstration evidence, and test-server configuration notes.

## Build

Build with a Java 21 JDK, as required by SDK main’s optional multi-release classes. Library code and tests target Java 8; the CLI and embedding applications run on Java 8 or later. Commands below run from this repository root:

```bash
./scripts/bootstrap-sdk.sh
./gradlew build writeDependencyReport
./gradlew spotlessApply
./gradlew check
./gradlew acceptanceTest
./gradlew installDist
```

SDK dependencies use ordinary Maven coordinates with `temporalVersion=1.40.0` in [gradle.properties](gradle.properties). Gradle's composite build substitutes the local `../sdk-java` main-branch projects for compilation. Generated Maven metadata retains the declared release version; release API compatibility is not checked in this POC.

`./scripts/bootstrap-sdk.sh` clones SDK `main` with its submodules only if the checkout is absent. Existing checkouts are used as-is: no SHA pin, reset, clean-worktree check, or revision validation. Use `TEMPORAL_SDK_DIR` or `-PsdkDir=...` to select another checkout. Keep that checkout on the main branch and update it through normal Git operations. Dependency versions and locking are managed by Gradle.

Use `--write-locks` deliberately when updating dependency locks. The Gradle wrapper is checked in. Generated outputs, package archives, cache contents, and SDK sources are excluded from this repository. The checked-in wrapper comes from sdk-java.

See [CONTRIBUTING.md](CONTRIBUTING.md) for SDK-aligned compiler, Error Prone, JUnit 4, formatting, versioning, publication and CI conventions. To run tests on an installed Java 8 JDK, use `./gradlew test -PtestJavaVersion=8`; the Gradle build itself remains on JDK 21.

## Repository structure

There are two production components:

| Component | Sources | Artifact |
| --- | --- | --- |
| Worker library, package model/tooling, runtime and CLI | `src/main/java/io/temporal/worker/packages/` | `io.temporal:temporal-worker-packages` |
| Packaging Gradle plugin | `build-logic/` | `io.temporal:temporal-worker-packages-gradle-plugin` |

The root Java packages separate API, tooling, runtime and CLI code within the same library JAR. The default library output is `build/libs/temporal-worker-packages-0.1.0-SNAPSHOT.jar`. All tests live in `src/test/java`; isolation fixtures live in `src/test/fixtures`. Runnable applications live in the separate [worker-packages-java-samples repository](https://github.com/mfateev/worker-packages-java-samples/blob/main/README.md). Library builds and tests do not require that checkout.

The main library's Maven dependency metadata includes the SDK dependencies needed by embedding applications. The local test-server dependency belongs to the CLI distribution and test harness; it is excluded from the library's runtime publication.

## Commands

After `installDist`, the executable is `build/install/temporal-worker-packages/bin/temporal-worker-packages`. Examples below call it `plugin` for readability; substitute that full path. Supply application archives in `./packages`; see the [samples repository](https://github.com/mfateev/worker-packages-java-samples/blob/main/README.md) for runnable greeting packages.

```bash
plugin help
plugin list --packages ./packages
plugin validate --archive ./packages/greetings-1.twp
plugin pack --manifest ./input/workflow-package.json --input-root ./input --output ./packages/greetings-1.twp
plugin server --port 7233
plugin worker --packages ./packages --package-version 2 --cache ./.package-cache --endpoint 127.0.0.1:7233 --namespace default --task-queue workflow-packages
plugin start --type GreetingWorkflow --workflow-id greeting-a --input '[]'
plugin signal --workflow-id greeting-a --name proceed
plugin result --workflow-id greeting-a --timeout-seconds 60
```

The CLI labels the SHA-256 of the exact `.twp` bytes as **archive checksum** in `list`, `validate`, and `pack` output. It is separate from the package version, which can be a Git commit SHA.

The example takes no arguments. Version 1 calls `greet(String name)`; version 2 changes it to `greet(String greeting, String name)` and adds `getName()`. A single `proceed` signal releases the workflow from its durable wait. See the [samples README](https://github.com/mfateev/worker-packages-java-samples/blob/main/README.md) for the complete comparison.

Flags support `--name value` and `--name=value`. Flags override environment defaults. Unknown/repeated flags and missing values fail clearly. Client inputs are JSON arrays of method arguments. Client results are printed as JSON. `--run-id` targets a particular run for signal/query/update/result; `--workflow-id` is always required.

| Setting | Flag | Environment | Default |
| --- | --- | --- | --- |
| Package directory | `--packages` | `TEMPORAL_PACKAGES` | `packages` |
| Prepared cache | `--cache` | `TEMPORAL_PACKAGE_CACHE_DIR` | `.package-cache` |
| Temporal endpoint | `--endpoint` | `TEMPORAL_ENDPOINT` | `127.0.0.1:7233` |
| Namespace | `--namespace` | `TEMPORAL_NAMESPACE` | `default` |
| Task queue | `--task-queue` | `TEMPORAL_TASK_QUEUE` | `workflow-packages` |
| Worker current / client requested version | `--package-version` | `TEMPORAL_PACKAGE_VERSION` | Worker: sole available version; client: omit header |

`--archive`, `--manifest`, `--input-root`, `--output`, `--type`, `--workflow-id`, `--run-id`, `--name`, `--input`, `--timeout-seconds`, and `--port` are command-specific flags without environment defaults. The result timeout defaults to 60 seconds and the test-server port to 7233. The `server` command is an ephemeral test service without persistent storage; keep it alive through worker restarts.

## Embed in a worker

For a source build, include this repository from the host's Gradle settings with `includeBuild("../worker-packages-java")`, then declare `implementation("io.temporal:temporal-worker-packages:0.1.0-SNAPSHOT")`. Its included SDK build supplies main-branch source artifacts. Prepare that SDK checkout first.

See the separate runnable [worker example](https://github.com/mfateev/worker-packages-java-samples/blob/main/worker/src/main/java/example/worker/PackageWorker.java) and its [launch instructions](https://github.com/mfateev/worker-packages-java-samples/blob/main/README.md#worker-implementation). It takes the package directory as its single command-line argument.

An empty `WorkerPackagePlugin.newBuilder().build()` is client-only. `setPackagesDirectory` enables worker loading; `setCacheDirectory` defaults to `.package-cache`. `setCurrentVersion` and `setVersionSelector` are alternative worker policies, with the last setting winning. `setClientVersion` is a separate optional client override and never changes the worker default. Cache or worker-policy settings without a package directory are rejected.

Prepare packages before creating or starting pollers. Use a distinct plugin instance per factory:

```java
import io.temporal.worker.packages.runtime.WorkerPackagePlugin;

WorkerPackagePlugin plugin = WorkerPackagePlugin.newBuilder()
    .setPackagesDirectory(Paths.get("packages"))
    .setCacheDirectory(Paths.get(".package-cache"))
    .setVersionSelector(context -> deploymentConfig.currentGitSha())
    .build();
WorkerFactory factory = WorkerFactory.newInstance(client,
    WorkerFactoryOptions.newBuilder().setPlugins(plugin).build());
try {
  factory.newWorker("workflow-packages");
  factory.start();
  // Keep the host running.
} finally {
  factory.shutdown();
  factory.awaitTermination(60, TimeUnit.SECONDS);
}
```

The selector receives workflow type, workflow/run IDs, workflow metadata, and all installed versions. It can select different versions for different new workflows or read changing host deployment configuration. It runs only inside the pin's side-effect callback; replay and activities use the recorded pin without calling it. A failed workflow task before commit may invoke it again. Missing, null, unsafe, or unsupported selections fail the workflow task without falling back or initializing package code. The callback must support concurrent workflows.

Without a current-version setting or selector, the plugin selects the sole available version. With multiple versions, starts without an explicit client version require a worker policy. The CLI and sample worker provide a fixed policy through `--package-version VERSION` and `TEMPORAL_PACKAGE_VERSION`, respectively. Diagnostics list supported types and the selection policy; a dynamic policy has no single global current version.

This POC's string-version manifest/header and single-side-effect history format replace the previous integer-version protocol. Rebuild old archives; replay compatibility with histories recorded by the previous format is not implemented.

The plugin intercepts factory shutdown and releases package loaders automatically after actual termination and any active saved-history replay. A daemon watcher observes completion outside the SDK's factory monitor; it never closes loaders while tasks are draining. A TODO tracks replacing this watcher with a future SDK `awaitTermination` callback. The normal SDK shutdown calls in the host are sufficient; no custom stop helper is needed.

If initialization fails before a factory is created, close the unattached plugin directly. Explicit `closeAfterTermination(factory)` remains available for callers that need synchronous loader release, and still rejects premature cleanup.

The plugin preserves existing worker interceptors and ordinary registrations. It installs dynamic workflow and activity dispatch on every worker in its factory. Task queues are configured only on SDK workers and workflow options, not on the plugin; use a separate factory for workers that need a different plugin configuration. Existing SDK preferred-version providers remain unchanged. Conflicts fail with a configuration diagnostic. The CLI creates a dedicated managed worker.

## Client and header protocol

Register the plugin on clients through SDK plugin options, following the same pattern as other Temporal plugins. A client-only plugin needs no package files and leaves the version header unset:

```java
WorkflowClient client = WorkflowClient.newInstance(service,
    WorkflowClientOptions.newBuilder()
        .setPlugins(WorkerPackagePlugin.newBuilder().build())
        .build());
```

`WorkerPackagePlugin` extends SDK `SimplePlugin`. Its client hook appends its interceptor while preserving existing interceptors. A client-only instance has no worker configuration or package loaders. Registering a worker-configured builder result on a client also lets the SDK propagate its worker configuration automatically to a factory created from that client.

Ordinary SDK clients need only a workflow type and task queue. The same builder can optionally request an explicit client version:

```java
import io.temporal.worker.packages.runtime.WorkerPackagePlugin;

WorkflowClient packagedClient = WorkflowClient.newInstance(service,
    WorkflowClientOptions.newBuilder()
        .setNamespace("default")
        .setPlugins(WorkerPackagePlugin.newBuilder().setClientVersion("2").build())
        .build());
WorkflowStub stub = packagedClient.newUntypedWorkflowStub("GreetingWorkflow",
    WorkflowOptions.newBuilder()
        .setWorkflowId("greeting-a")
        .setTaskQueue("workflow-packages")
        .build());
stub.start();
```

The interceptor is optional: `WorkflowClient.newInstance(service)` starts workflows using the worker’s current version. To request a particular version, set `setClientVersion("2")` on the plugin builder, as above; no package directory or package ID is required. Typed clients use that same client and their ordinary `@WorkflowMethod` names. Signal-With-Start preserves the start header. Existing signals, queries, and updates cannot change a run's selected package.

The reserved key `container.package` contains a default-converter `json/plain` payload:

```json
{"formatVersion":1,"packageVersion":"2"}
```

This workflow start header is optional. An explicit version bypasses the worker selector. Otherwise configure the worker plugin builder with `setPackagesDirectory(...)` and either `setCurrentVersion("2")` or a `setVersionSelector(...)` callback. With neither, a sole available version is selected automatically. A missing requested type/version fails the workflow task rather than falling back. The worker records `[version, workflow archive digest, archive name]` in one side-effect marker before application commands; replay never consults the current version or callback. Each regular/local activity resolves its own archive by activity type and the pinned logical version:

```json
{"formatVersion":1,"packageId":"greetings","packageVersion":"2","sha256":"<64 lowercase hexadecimal characters>"}
```

Type names stay `GreetingWorkflow` and `Greet`, for example, across versions and package IDs. No package metadata is inserted into application arguments. Header schemas reject unknown/missing fields, unsupported format versions, invalid numbers/digests, and conflicting reserved metadata. Other headers are preserved. An activity requires its recorded package reference; an explicit patch may override that reference for execution. It never falls back to a current version. The activity interceptor's dispatch adapter is in-memory only and leaves wire payloads and application signatures unchanged.

## Packages, isolation, and limits

See [SPEC.md](SPEC.md), [manifest schema](src/main/resources/workflow-package.schema.json) and [header schema](src/main/resources/package-header.schema.json). Each `.twp` is a deterministic ZIP containing a strict manifest and its ordered JARs. Packages contain no SDK revision requirement. Legacy `temporalSdkRevision` metadata is accepted and ignored; host/package compatibility policy will be designed separately. Package versions are opaque strings, such as Git SHAs or semantic versions. Multiple archives can share a version, including archives with the same metadata name, as long as their exported types do not overlap. Valid tokens match `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`. Listing sorts lexically for stable output; this order never chooses a version. Duplicate workflow types or duplicate activity types at the same version fail before worker startup, with both archive paths in the error. Workflow and activity names have separate namespaces. Activity-only archives are supported. The manifest’s `packageId` is an internal archive label, not a client routing key. Empty listing is valid; an empty worker runtime fails. Adding archives takes effect after restart. Applications apply the `io.temporal.worker-packages` Gradle plugin from `build-logic/`. Its `workerPackage` task produces a validated `.twp` in the application's `build/libs/`, and participates in `assemble` and `build`. See the separate [sample build configuration](https://github.com/mfateev/worker-packages-java-samples/blob/main/README.md).

Validation rejects unsafe paths/symlinks, duplicate types and classes, host-owned classes, undeclared files, JAR `Class-Path` and `META-INF/INDEX.LIST`, including reserved definitions hidden in multi-release layouts. Streaming limits default to 256 MiB archive bytes, 1 GiB expanded bytes, 1 MiB manifest, and 100,000 entries. Embedders configure `ValidationLimits` when constructing the validator/catalog. Cache publication uses a cross-process lock and verifies previously prepared content.

The isolated loader is child-first for application classes/resources and parent-only for host namespaces. `ReservedNamespaces` is the authoritative list. Packages compile against shared SDK, protobuf/gRPC, Guava, Nexus, SLF4J, metrics, and Jackson annotation types without bundling them. Guava is shared because gRPC future-stub APIs exposed through the activity context's workflow client return `ListenableFuture`; the SDK pins Guava 33.4.8-android; dependency resolution selects 33.4.8-jre through gRPC, which is the shared runtime version recorded in the report and locks. Private Jackson core/databind classes may remain isolated. The host uses its default converter on every supported payload path.

Workflow instances are fresh per reconstruction. Activity instances are shared per package digest and implementation class and must support concurrent calls. Loaders stay open until workers terminate. Without an explicit patch, missing historical packages or replaced bytes fail workflow tasks and allow recovery after restoring the exact archive. An explicit patch redirects execution as described below.

## Patched packages

Add `patchesVersion` to an application's existing Gradle package configuration:

```groovy
workerPackage {
    packageId = 'greetings'
    packageVersion = 'patch1'
    patchesVersion = 'base-sha'
    // Keep the workflow and activity exports configured here.
}
```

This produces `greetings-base-sha-patch1.twp`. Its `workflow-package.json` contains `"packageVersion": "patch1"` and `"patchesVersion": "base-sha"`. Patch filenames must match `<packageId>-<patchesVersion>-<packageVersion>.twp`; metadata supplies the identities, so names containing hyphens are not parsed by splitting the filename.

After restarting the worker with this archive installed, workflows pinned to `base-sha` execute `patch1`. The original archive may be absent. Workflow history and regular/local activity headers retain their original version and checksum; workflow dispatch and activity attempts resolve that reference to the installed patch. Existing workflows do not call the version selector again. Completed activities keep their recorded results; patches affect replay code and subsequent activity attempts.

Patches can themselves be patched. For `base-sha → patch1 → patch2`, install both `greetings-base-sha-patch1.twp` and `greetings-patch1-patch2.twp`. The second archive declares `"packageVersion": "patch2"` and `"patchesVersion": "patch1"`. Runs pinned to either `base-sha` or `patch1` execute `patch2`. Keep intermediate patch archives installed because their metadata describes the chain; the original base archive is optional. Removing an intermediate archive removes that edge rather than inferring it from filenames or version ordering.

The selector can also choose a logical target whose original archive is absent. Its initial pin records that target version and the effective patch's checksum. Available versions in selector context include installed versions and patch target aliases; configure an explicit selector for such deployments. New runs selecting a physical version retain that version's original reference before patch resolution.

Each workflow/activity type at a target version has at most one direct patch. Startup rejects competing patches for the same type/target, self-patches, and cycles. Disjoint types may share patch versions and use different archive labels. The CLI lists each archive's `PATCHES VERSION`, and plugin diagnostics include `patchesVersion`.

A patch is an explicit deployment override of the original pinned code and archive checksum. The worker validates and prepares the patch's own archive bytes. Without a patch, original checksum matching remains strict. Patch metadata does not authenticate its publisher. Patch workflow code must replay the existing history deterministically and preserve the payload/activity contracts required by that history; incompatible changes still produce normal Temporal replay failures.

## Validation and samples

Run the independent library checks from this repository:

```bash
./gradlew build installDist
./gradlew test -PtestJavaVersion=8
./gradlew acceptanceTest
./scripts/validate-clean-checkout.py
```

Library tests retain internal isolation, replay, retry, payload and lifecycle fixtures under `src/test/fixtures`. The included `build-logic/` tests validate packaging through Gradle TestKit. The clean-checkout script snapshots only this repository, bootstraps a separate SDK checkout, and verifies the library without a samples checkout.

Runnable greeting applications, the standalone worker example, their two integration tests, the separate-worker restart demo, and the CLI smoke script live in [worker-packages-java-samples](https://github.com/mfateev/worker-packages-java-samples/blob/main/README.md). That repository has its own Gradle wrapper and includes the library and SDK as local composite builds. Its `build` and `demo` commands are separate from this library's validation.

The SDK Java test server does not expire abandoned sticky queues. Integration tests request full-history normal-queue tasks using a public gRPC interceptor; production plugin behavior is unchanged. See the acceptance report for details and actual results.

## Prototype boundaries

This is trusted-code classloader isolation, not an untrusted-code sandbox. No SDK internals are imported or modified. Remote downloads, live directory refresh, server-driven selection, rollout/rollback, hot unloading, Spring/DI, package converters, and Worker Deployment Versioning are excluded. Child workflows, Continue-As-New, workflow retry policies, cron, schedules, reset migration, and Update-With-Start are unsupported. Cross-package/shared activities and version inheritance across new runs are excluded. Workers sharing a task queue must have the same package snapshot.
