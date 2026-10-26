# Temporal Worker Packages implementation specification

Implement a runnable prototype that hosts multiple versions of packaged Temporal Java workflows and activities in one JVM. Discover versions in a local directory, select a string version for each new workflow through a host callback, and keep each workflow run on its original package through replay, activity retries, and worker restarts.

This is the package and runtime implementation contract, updated for the approved header-routing design. See README.md and ACCEPTANCE_REPORT.md for implementation and verification status. The plugin is maintained in its own Git repository, with an SDK main checkout as its sibling.

## 1. Scope and fixed decisions

| Area | Prototype requirement |
| --- | --- |
| Runtime | Java 8 or later (build with JDK 21); Gradle wrapper; one worker JVM hosting multiple package versions |
| SDK | Compile against sdk-java main through Gradle; declare released Maven versions directly in Gradle |
| Package | ZIP archive with `.twp` extension, JSON manifest, and ordinary JARs |
| Versions | Opaque string tokens, including Git SHAs and semantic versions; no implicit deployment ordering |
| Current selection | Host version-selector callback per new workflow; sole-installed fallback |
| Entrypoints | Manifest-listed classes implementing annotated Temporal interfaces; no application `main()` |
| Dispatch | Stable host `DynamicWorkflow`, `DynamicActivity`, and workflow interceptors registered before worker startup |
| Isolation | One application class loader per archive digest; one shared host SDK |
| Pinning | One recorded side effect for the selected string version and archive digest |
| Availability | Local archives prepared before polling; application classes and instances loaded on first use |

Do not modify SDK internals. Use public APIs from the sdk-java main source checkout through Gradle's composite build. Declare released Maven dependencies in Gradle; no source SHA pin or runtime SDK revision comparison is required. Host/package compatibility policy is deferred for this POC.

Out of scope: remote package downloads, live directory refresh, server-driven current selection, rollout or rollback controls, hot unloading, Spring/DI integration, arbitrary package converters, untrusted-code sandboxing, and Temporal server Worker Deployment Versioning. Child workflows, Continue-As-New, workflow-level retry policies, cron runs, schedules, and reset-based migration are not supported in this first prototype. Document these limits, reject unsupported outbound child/Continue-As-New calls clearly, and do not claim version inheritance across runs.

## 2. Required project deliverables

Publish two production components: the root `io.temporal:temporal-worker-packages` library and the included `build-logic/` packaging Gradle plugin (`io.temporal:temporal-worker-packages-gradle-plugin`). The library contains API, tooling, runtime and CLI Java packages under `src/main/java/io/temporal/worker/packages/`. Put all tests under `src/test/java` and isolation fixtures under `src/test/fixtures`. Include:

1. Package model, JSON schema, archive validator, directory catalog, and packager.
2. Container runtime, class loader, dispatch adapters, and version-aware interceptor.
3. CLI for listing packages and running a worker, plus an example client.
4. Internal conflicting-dependency fixtures under `src/test/fixtures`; independently runnable greeting applications and a worker host in the separate [samples repository](https://github.com/mfateev/worker-packages-java-samples/blob/main/README.md).
5. Unit and Temporal integration tests.

Use dependency locking or equivalent explicit version pinning. Keep SDK checkouts, prepared archives, and other generated files out of Git. Include commands in `README.md` for bootstrap, build, tests, package generation, package listing, worker startup, and the demonstration. The test suite must run without a separately installed Temporal server using `TestWorkflowEnvironment`.

## 3. Package format

An archive contains exactly one root `workflow-package.json` and JARs identified by an ordered classpath:

```text
greetings-2.twp
  workflow-package.json
  app/greetings.jar
```

Normal archive filenames are informational. Patch archives must use `<packageId>-<targetVersion>-<patchVersion>.twp`; manifest fields define identity, version and patch target.

```json
{
  "formatVersion": 1,
  "containerApiVersion": 1,
  "packageId": "greetings",
  "packageVersion": "2",
  "javaRelease": 8,
  "payloadEncoding": "temporal-default-v1",
  "classpath": ["app/greetings.jar"],
  "workflows": [
    {
      "type": "GreetingWorkflow",
      "implementation": "example.greeting.GreetingWorkflowImpl"
    }
  ],
  "activities": [
    {
      "type": "Greet",
      "implementation": "example.greeting.GreetingActivitiesImpl"
    },
    {
      "type": "GetName",
      "implementation": "example.greeting.GreetingActivitiesImpl"
    }
  ]
}
```

Required validation:

- Require all illustrated fields; reject unknown fields for manifest version 1 except optional legacy `temporalSdkRevision`, which is ignored.
- Require `formatVersion` and `containerApiVersion` to equal 1, `javaRelease` to equal 8, and the illustrated payload encoding.
- Require a string `packageVersion` matching `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`, including Git SHAs and semantic versions; reject numeric values and unsafe or empty tokens.
- Restrict package IDs and logical type names to `[A-Za-z][A-Za-z0-9_.-]{0,99}`. Require nonempty unique workflow types, unique activity types, and at least one workflow. Activities may be empty.
- Require a nonempty ordered classpath of unique relative `.jar` paths, all present in the archive. Require valid implementation binary names.
- Reject every declared JAR containing a manifest `Class-Path` attribute, including an empty attribute, or `META-INF/INDEX.LIST`. The package manifest is the sole source of application classpath entries and their order; JAR metadata must not add external JARs or directories or reorder declared entries. Report the offending JAR and attribute or entry. The packager must reject these inputs rather than silently rewriting their bytes.
- Reject duplicate JSON keys, duplicate ZIP entry names, absolute paths, parent traversal, symlinks, and extraction outside the prepared directory. Apply the same path checks to nested JAR entries when inspecting them.
- Reject extra non-directory archive files other than the manifest and declared JARs. Reject duplicate class definitions across the declared JARs in a package.
- Reject classes in host-owned namespaces described in section 5. Do not initialize application classes during archive inspection.
- Bound archive size, expanded size, manifest size, and entry count. Document configurable defaults; suggested defaults are 256 MiB compressed, 1 GiB expanded, 1 MiB manifest, and 100,000 entries including inspected JAR entries. Enforce limits while streaming, not solely from ZIP metadata.

Compute SHA-256 over the complete archive bytes. The lowercase 64-character digest is the content identity. Do not put that digest inside the archive itself. The packager must use stable entry order and timestamps so repeated builds with identical input JARs produce identical archives.

## 4. Discovery and version selection

Configuration must include package directory, prepared-cache directory, Temporal endpoint, namespace, and task queue. Provide these through CLI flags with environment-variable defaults and document precedence. Use one task queue for the prototype's workflow and activity workers.

At startup, scan only direct regular `.twp` files in the package directory; ignore other extensions and do not follow symlinks. A nonexistent or unreadable directory is an error. Validate every candidate and build an immutable catalog:

```text
(packageId, packageVersion) -> archive digest and prepared classpath
packageId -> available versions in deterministic lexical order
```

Reject duplicate version strings globally, including archives with different package IDs or identical archives under different filenames, with both paths in the diagnostic. If any candidate fails validation, fail startup before polling. An empty directory is valid for the listing command; running a worker requires at least one package.

Each package version is a complete deployment. New runs use the version returned by the host selector; if that version lacks a requested workflow type, report an unsupported type rather than falling back to an older version. An older run resolves its own version's manifest. Different package IDs may expose the same logical workflow or activity type.

The `list` command prints package ID, string version, digest, and archive path in deterministic lexical order. Ordering never chooses an implementation.

No rescan occurs while a worker is running. Adding a version takes effect after restart. Workers sharing a task queue must receive the same immutable package set; automatic coordination is future work. Expose a version-selector function through plugin options. Its context includes package ID, workflow metadata, and all installed versions. Without a function, only a sole installed version may be selected. Do not implement a deployment server now.

### Explicit patch resolution

Optional string `patchesVersion` declares a version in the same package that this archive replaces for execution. A patch archive's filename must be `<packageId>-<patchesVersion>-<packageVersion>.twp`; ordinary archives retain their existing naming behavior. Omit the field for normal packages; null, empty, unsafe, or self-target values are invalid.

Build a target-to-patch map per package at startup. Reject multiple direct patches for the same target, cycles, and references to installed versions belonging to other package IDs. Global archive-version uniqueness still applies. Follow patch chains to their leaf; ordering and the host selector do not choose between patches.

A previously pinned reference is resolved through this map before strict original archive/checksum lookup. An explicit patch intentionally overrides the original checksum; the effective patch's own bytes are validated and prepared. With no patch edge, exact original version and digest matching remains mandatory. Original archives may be absent. Intermediate patch archives must remain installed to describe chain edges.

Retain the original history pin and activity headers during replay and scheduling. Resolve both workflow execution and each regular/local activity attempt to the effective patch. For a new workflow selecting an absent logical target, record that target version with the leaf patch checksum; if its physical original is present, retain the original reference. The selector's available-version list includes patch target aliases. Deployment changes take effect after worker restart, and patch code must remain replay-compatible with existing history.

## 5. Archive preparation and class loading

Before starting pollers, copy verified archive bytes into a digest-addressed cache and extract the declared JARs there. Verify the copied archive digest and publish prepared directories atomically. Concurrent container processes must not see partial extraction; use a cache lock or equivalent atomic preparation protocol. Do not overwrite a prepared digest directory with different content. Container-owned prepared files must remain intact for the worker's lifetime.

Create a loader descriptor for each archive at startup. Defining application classes, resolving reflective entrypoint metadata, and constructing instances may be deferred until first use. Normal JVM class loading from already prepared local JARs is permitted. Downloads, extraction, directory scans, archive hashing, and waits for a host background loader are forbidden on workflow threads. Do not implement lazy loading by polling an external future in `Workflow.await`, by suppressing deadlock detection, or by scheduling a preparatory activity whose side effects are assumed to exist during replay.

Use child-first loading for application classes and dependencies. Host-owned classes use parent-only resolution, with no fallback to a package copy. At minimum reserve `java.*`, `javax.*`, `jdk.*`, `sun.*`, `com.sun.*`, `io.temporal.*`, the container API namespace, `com.google.protobuf.*`, `io.grpc.*`, `org.slf4j.*`, `com.uber.m3.*`, and `com.fasterxml.jackson.annotation.*`; packages must not contain classes in those namespaces. Keep this list centralized and documented. A small versioned host container API should live under a dedicated namespace such as `io.temporal.worker.packages.api`.

The shared boundary includes third-party types exposed by supported SDK APIs and annotations interpreted by the host converter. In particular, workflow and activity metrics use the host Tally API and its M3 types, and JSON DTOs use the host Jackson annotations. Audit supported public SDK signatures, including the signatures of returned interfaces, for other types that require shared identity. Pin the host dependencies providing those types and publish their versions for package builds; packages must compile against them without bundling their classes. Private Jackson core/databind dependencies may remain isolated for application-internal use, but must not replace the host converter or the shared annotation classes.

Construct each loader using only the verified prepared JARs in the package manifest's classpath order. Enforce the JAR metadata restrictions in section 3 before constructing a loader; an external file must not become executable package code merely because it exists beside the cache or at a manifest-referenced URL.

The loader's parent must not contain example application classes or their private dependency fixtures. Resource lookup must prefer package resources, with deterministic ordering and duplicate URL suppression for `getResources`; service descriptors under `META-INF/services` follow the same ordering. Pass the selected loader explicitly to `ServiceLoader` when needed. Do not rely on a JVM-wide mutable context class loader or a static current-package variable.

Keep one loader per digest for the life of the container. Cache immutable metadata, never workflow instances. Preserve and restore the thread context class loader around package callbacks where needed, including activity and handler calls. Workflows may yield and handlers may run on different workflow threads: package selection must reside in per-workflow state, not an ordinary thread-local. Do not leave process-wide serialization caches retaining conflicting application classes by name alone.

Close loaders after worker shutdown and activity draining. Hot eviction and guaranteed class unloading are out of scope. Application static state is shared within one version and must not influence workflow determinism.

## 6. Application entrypoint contract

Packages do not run a `main()` method, create workers, or receive a raw `Worker` to register themselves. The manifest acts as the deployment descriptor.

For workflows:

- Each declared implementation must be a public concrete class with a public zero-argument constructor and implement a Temporal `@WorkflowInterface`.
- Require exactly one workflow entry method per implementation for this prototype. Its SDK-resolved workflow type must equal the manifest's logical `type`.
- Support `@WorkflowMethod`, `@SignalMethod`, `@QueryMethod`, `@UpdateMethod`, and `@UpdateValidatorMethod`, including annotations inherited through interfaces.
- Use public SDK metadata APIs such as `POJOWorkflowImplMetadata` to follow Temporal naming rules. Do not duplicate its annotation/name resolution algorithm.
- Construct a fresh instance for each run reconstruction, then register that instance through `Workflow.registerListener` and invoke its workflow entry method.
- `@WorkflowInit`, custom workflow factories, dynamic application workflows, and dependency injection are unsupported; reject their use with an actionable diagnostic.

For activities, require public concrete implementations with public zero-argument constructors and `@ActivityInterface` interfaces. Use `POJOActivityImplMetadata` to resolve method types, including activity name prefixes and explicit names. Every declared activity type must resolve to exactly one method on its implementation. Permit multiple manifest entries to refer to different methods of the same class; instantiate that class once per package per container and share it across calls, following normal activity concurrency expectations. Only listed activity methods are exported.

Invoke reflected methods with the SDK-decoded arguments and unwrap `InvocationTargetException` to preserve the application's real failure semantics. Support void methods, multiple arguments, and generic parameter types. Do not turn all exceptions into terminal application failures.

## 7. External names and durable run selection

External workflow types equal their logical manifest/annotation names, for example `GreetingWorkflow`. The reserved `container.package` workflow start header contains `formatVersion: 1` and `packageId`. Typed and untyped clients use the explicit package header helper. Package identity is never encoded in a workflow or activity type. The header and durable pin protocol is documented in README.md.

The dispatcher invokes the plugin options' version selector inside a single `Workflow.sideEffect` callback. Before any application constructor, listener registration, or application command, that callback validates the returned string token, resolves the requested package and version, verifies the workflow type is exported, and returns `[version, archiveDigest]` as a `String[]`.

The callback may read host deployment configuration and choose different versions for different new workflows. It must support concurrent calls. It is never evaluated while replaying a recorded pin. The plugin leaves SDK preferred-version providers unchanged; application `Workflow.getVersion` behavior remains host-controlled.

The side effect becomes durable only when the workflow task commits. A failed first task may select another version on its next attempt. Null, invalid, missing, or unsupported selections fail the workflow task without initializing application code or falling back.

On replay, resolve the recorded string version through explicit patch edges first. Without a patch, lookup or digest mismatches must fail the workflow task and permit recovery after restoring the required archive, regardless of the selector. Activities inherit the same full pin in their headers.

This replaces the earlier integer `getVersion` marker plus digest-only side effect. Compatibility with that previous manifest/header/history format is outside this POC; old archives must be rebuilt.

## 8. Workflow dispatch and handlers

Register a host dynamic workflow implementation and the container interceptor before starting the worker. For a dynamic workflow, `EncodedValues` carries arguments. Decode each argument using the selected reflected method's parameter `Class` and generic `Type`, then invoke the selected instance. Use the SDK's default data converter throughout; `temporal-default-v1` means that fixed host converter configuration, with no package-supplied codecs or converter plugins.

Use default-converter-compatible payloads, including plain JSON DTOs, lists, and maps. Resolve DTOs using the selected package's classes. JSON DTOs must not embed Java class names requiring arbitrary polymorphic class loading. Wire-compatible client DTOs may be separately compiled. Do not add a hidden envelope or argument that changes application method signatures.

Supported Jackson annotations on DTOs, such as `@JsonProperty` and `@JsonIgnore`, must resolve through the shared `com.fasterxml.jackson.annotation.*` namespace so the host mapper recognizes them. Declare the pinned host annotation dependency as compile-only in application packages. Bundled copies must fail archive validation rather than silently losing annotation behavior. Package-supplied Jackson modules and custom serializer/deserializer extensions to the host converter are unsupported. Verify annotated DTO behavior on workflow, activity, and handler payload paths, including replay reconstruction.

Set the selected `PackageRef` in per-workflow interceptor state before constructing application code so activity calls from constructors or delivered handlers use the right package. Register annotated handlers using `Workflow.registerListener(packageInstance)`. Ensure buffered signals are delivered to that instance after selection, and queries and updates after initialization reach the same object. Preserve update validator rejection and query read-only semantics. Queries before initialization may return the SDK's normal unavailable-query error. Update-With-Start is out of scope.

Exercise handlers after a cache eviction or worker restart, not only while the initial instance is cached. Framework wrappers must not swallow SDK cancellation or workflow-thread destruction errors.

## 9. Version-aware activity dispatch

Applications use ordinary typed or untyped activity stubs with unchanged logical activity names. A workflow outbound interceptor adds the `container.package` header containing formatVersion 1 and the pinned packageId, packageVersion, and full archive sha256. Unrelated header payloads remain intact; conflicting reserved metadata is rejected.

For both `executeActivity` and `executeLocalActivity`, preserve argument objects, result class and generic type, options, retry settings, headers, and explicit activity IDs. Use the caller workflow's pinned `PackageRef`. Unknown logical activity names fail clearly; cross-package and independently deployed shared activities are out of scope.

The activity inbound interceptor validates the package header and bridges the reference to the host dynamic activity, which uses the unchanged logical type, resolves any explicit patch or otherwise verifies the original package identity and digest, obtains the effective implementation, decodes parameters using that package's reflected types, and invokes the activity under the SDK's activity execution context. Preserve heartbeats, application exceptions, and cancellation. Results pass back through the default converter. Void methods must complete successfully for void callers; the dynamic dispatcher's null-result encoding need not be byte-identical to the SDK's ordinary POJO void encoding.

An activity worker must obtain everything needed for routing from the activity task and its own catalog. It must not query workflow history or consult a local workflow-instance cache. Missing packages are retryable activity failures subject to the activity's configured retry policy, not a reason to run a different version. Logs must identify the required package and digest without dumping payloads.

## 10. Errors and diagnostics

Use distinct errors for malformed archives, unsupported manifests or entrypoints, duplicate version identifiers across all package IDs, unknown workflow/activity types, missing versions, digest mismatches, and unsupported features. Include package ID, version, digest where available, and the relevant file or logical type. Avoid logging complete workflow inputs or outputs.

Startup archive errors fail before polling. Entrypoint errors discovered on first use fail the workflow task; document how to reproduce validation against the affected package. Missing-package failures must remain distinguishable from business exceptions. Do not configure `failWorkflowExceptionTypes(Throwable.class)` or otherwise make every container exception terminal.

## 11. Acceptance tests

Use real `.twp` archives and isolated classpaths. Packaging application classes onto the test JVM's parent classpath invalidates the isolation tests. SDK-backed tests may use `TestWorkflowEnvironment`; use one shared test service across worker restarts so recorded history survives.

| Test | Required observation |
| --- | --- |
| Numeric discovery | Versions 2, 9, and 12 select 12 despite misleading filenames and enumeration order; different package IDs select independently. |
| Validation | Malformed manifests, duplicate versions, unsafe paths, duplicate classes, bundled host SDK classes, and archive limits produce actionable failures. Empty-directory behavior matches section 4. |
| Declared classpath only | Reject JAR manifest `Class-Path` attributes, including relative paths, absolute `file:` URLs, references to declared JARs, and empty attributes; reject `META-INF/INDEX.LIST`. Place a fixture JAR outside the archive and prove a manifest reference cannot load or initialize its classes. Errors identify the offending declared JAR and metadata before polling. |
| Packaging | Identical input manifests and JARs produce byte-identical archives and identical digests. |
| Concurrent versions | Start A while only version 2 is installed. Add version 12 and restart the worker against the same service. A continues on 2 while new run B executes 12 in that same worker JVM. |
| Class isolation | Both archives contain identical application class names and incompatible versions of a private dependency. Both return their own expected behavior; both use the same host `Workflow.class`. |
| Shared metrics API | Workflow and activity implementations in both isolated package versions report a metric through their normal SDK execution contexts using the same host Tally/M3 classes, without loader-constraint failures. An archive bundling classes in `com.uber.m3.*` is rejected before polling. |
| Durable selection | Inspect history to verify the single side-effect marker containing string version and digest precede application commands. Restart with a cold container registry and prepared cache and reproduce A's result. |
| Replaced archive | After recording A's selection, replace version 2 with different bytes. Replay must report a digest mismatch and must not invoke replacement workflow or activity code. |
| Missing old version | Remove version 2 while retaining 12. A's workflow task fails for its missing package; B can still run on 12. Restore original version 2, restart, and A resumes on 2. |
| Activity retry elsewhere | Force an activity attempt on worker process A to fail, stop A, then execute its retry on a fresh worker process B containing the original package and a higher current version. Its unchanged logical type and original package header survive; it uses the original digest and implementation. |
| Local activities | Logical names stay unchanged and headers carry the run pin, results replay correctly, and a higher current version does not change old-run behavior. |
| Handlers | A buffered signal, a query, an update, and validator rejection reach the selected version. Repeat handler operations after replay reconstruction. |
| Payloads | DTO classes available only through package loaders, generic collections, multiple parameters, void methods, application failures, and cancellation retain expected SDK behavior. |
| Shared JSON annotations | Package-only DTO classes using shared `@JsonProperty` and `@JsonIgnore` retain their renamed and excluded fields in encoded workflow/activity inputs and results and signal/query/update payloads. Inspect payload field names and repeat after replay reconstruction. Reject an archive bundling Jackson annotation classes before polling, including when it also contains a private Jackson dependency version. |
| Provider scope | The plugin leaves an existing SDK preferred-version provider unchanged; application `Workflow.getVersion` retains host behavior. |
| Lazy definition | Discovery and archive preparation do not initialize application classes. First dispatch initializes only the selected package's entrypoint/dependencies. Concurrent first use shares one loader and valid metadata. |
| Shutdown | Pollers and activities drain before loaders close; no container-owned loader executor or worker threads leak. |

For cold-registry tests, reconstruct the container and its runtime objects rather than only creating a new client stub. Include an explicit saved-history replay test in addition to live worker resumption. For the cross-process activity test, provide a subprocess test or integration harness; a second object sharing the first worker's static state does not prove independence.

## 12. Separate samples demonstration and definition of done

The separate [samples repository](https://github.com/mfateev/worker-packages-java-samples/blob/main/README.md) provides its own build, tests and demonstration script that:

1. Build greeting versions 1 and 2 with identical fully qualified class names. Both workflows use zero-argument `run()` and a `proceed()` signal. Version 1 calls `greet("World")` after the signal. Version 2 calls the new `getName()` activity before waiting, then calls the changed `greet(String greeting, String name)` with `"Hi"` and the returned name after the signal.
2. Put only version 1 in a package directory and start a worker.
3. Start workflow A and pause it through a durable workflow wait.
4. Add version 2, restart the worker, and list both available versions, and configure the host selector to choose 2 for new runs.
5. Start workflow B and signal both runs. A returns `Hello, World!` using its original activity signature; B returns `Hi, multi-versioned worker!` using the changed signature and added activity.

Keep the broader incompatible-dependency and payload scenarios under `src/test/fixtures/`, separate from this minimal public example.

The library must bootstrap SDK main, resolve its locked dependencies, and pass its tests from a clean checkout with no samples checkout present. Independently validate the samples repository by building both application packages, passing its two integration tests, and running its demonstration using documented commands. Report actual test commands and results, SDK/API limitations, and unsupported features. Do not present a happy-path demonstration as proof of replay or dependency isolation without the corresponding tests.

Keep server-driven selection, remote loading, rollout/rollback, cross-run inheritance, richer lifecycle initialization, and hot unloading on a clearly labeled future-work list. Avoid implementing those features to finish this prototype.

## Source references

SDK main provides these extension points. Read its applicable `AGENTS.md` instructions when working in that checkout.

- [PreferredVersionProvider contract](https://github.com/temporalio/sdk-java/blob/main/temporal-sdk/src/main/java/io/temporal/worker/PreferredVersionProvider.java)
- [Dynamic workflow dispatch and result conversion](https://github.com/temporalio/sdk-java/blob/main/temporal-sdk/src/main/java/io/temporal/internal/sync/DynamicSyncWorkflowDefinition.java)
- [Annotated listener registration](https://github.com/temporalio/sdk-java/blob/main/temporal-sdk/src/main/java/io/temporal/internal/sync/WorkflowInternal.java#L129)
- [Workflow and activity metadata](https://github.com/temporalio/sdk-java/tree/main/temporal-sdk/src/main/java/io/temporal/common/metadata)
- [Outbound activity interception](https://github.com/temporalio/sdk-java/blob/main/temporal-sdk/src/main/java/io/temporal/common/interceptors/WorkflowOutboundCallsInterceptor.java)
- [Side effects and workflow version API](https://github.com/temporalio/sdk-java/blob/main/temporal-sdk/src/main/java/io/temporal/workflow/Workflow.java)
