# Acceptance suite

The independent library suite contains **68 Java 8-compatible tests**: 31 integration, 16 runtime and 21 tooling. The included `build-logic` build contains **12 Gradle TestKit tests**, for **80 passing library tests** after the samples extraction; see the [acceptance report](../../ACCEPTANCE_REPORT.md) for completed results. Select an installed test runtime with `-PtestJavaVersion=8` (or 11/17/21); fixture compilation and worker subprocesses use that runtime. Gradle and TestKit run on JDK 21.

The two greeting/worker integration tests, restart demo and CLI smoke script now belong to [worker-packages-java-samples](https://github.com/mfateev/worker-packages-java-samples/blob/main/README.md). Library tests use only the internal fixtures in this repository and do not require the samples checkout.

Version selectors run inside a single side effect recording the string version and archive digest. New integration tests verify per-run host choices, unchanged old pins through cold restart and saved replay, activity header propagation, and recovery from null or unavailable selections before pin/application execution. The retry test sets a two-second workflow-task timeout to observe SDK timeout-based retries; repeated failures may not add separate failure events to history.

| Source test | Coverage |
| --- | --- |
| `PatchDispatchIntegrationTest` (3) | Cold patch-only chain dispatch/replay, unchanged old pins and headers, unaffected unrelated versions, missing-base logical selections, and activity retry in a fresh JVM using patch-only code. |
| `VersionSelectorIntegrationTest` (2) | Git-SHA and opaque selection per new workflow, policy changes, immutable context, single atomic pin, cold resume/saved replay bypassing callback, old activity headers, and null/unavailable selection failures with live recovery. |
| `HeaderContractTest` (2) | Strict versioned headers; malformed versions/digests, duplicate/extra fields, encoding, missing/conflicting metadata, unrelated payload bytes. |
| `ClassLoaderIsolationTest` (1) | Host fixture absence; identical private names and incompatible dependency APIs; shared SDK/metrics/annotation identity; resources and actual ServiceLoader providers; no initialization during preparation. |
| `VersionIsolationTest` (12) | Durable version/digest markers and order; application patch scope; cold restart, saved replay, concurrent old/new versions; missing/replaced archive diagnostics and constructor probes; recovery and first-task failure before commit; typed/untyped and Signal-With-Start starts; handlers, validator/query read-only semantics, package DTO results and annotations; handler-originated activities; generic/void payloads; regular/local and typed/untyped activities, IDs/options/opaque headers, export validation, local old pins, business failures, heartbeat cancellation, concurrent activity singleton initialization, unsupported child/Continue-As-New rejection. |
| `CrossProcessRetryTest` (1) | A retryable activity failure in worker JVM A; shutdown A; retry in fresh JVM B with original/new packages; original header/digest/code retained; new run selects current. |
| `PluginLifecycleIntegrationTest` (6) | SDK provider preservation and actual dynamic registration conflicts; unrelated typed worker/activity/provider pass-through; hook order/forwarding; failed-start cleanup; loaders usable during graceful activity draining; queue-named thread termination. |
| `HeaderDispatchIntegrationTest` (3) | Malformed workflow headers fail tasks before pinning/application execution; malformed activity headers reach the dynamic worker and fail without fallback; missing activity packages remain retryable through configured exhaustion. |
| `PrivateJacksonIntegrationTest` (1) | Bundled private Jackson core/databind 2.14.3 coexist with host Jackson 2.15.4 and shared annotations; real workflow serialization; host default converter remains active. |

`FixtureCompiler` independently compiles stress-test sources from `fixtures/` and private dependencies targeting Java 8,
produces deterministic JARs and validated archives, and never puts application classes on the host
classpath. The private Jackson configuration supplies archive inputs only; it does not extend the
host runtime classpath. `ProcessHost` checks readiness,
exit status, and bounded cleanup. All service/process/runtime resources have cleanup paths.

## In-memory test-server routing limitation

This SDK's Java test server saves sticky task queue names but neither expires their
schedule-to-start timeout nor implements the public `ResetStickyTaskQueue` RPC. A new worker has a
new sticky queue UUID; the old queue otherwise remains unreachable. The original restart tests
exposed this behavior as timeouts with no replay failures.

`TestServices` configures only acceptance/demo service clients with a public gRPC interceptor that
declines sticky execution by clearing sticky attributes on workflow task completion requests.
Workers therefore receive real complete histories through normal queues and reconstruct across
cold factories, runtimes, caches, and JVMs. Application commands, version/digest markers, payloads,
and routing headers are unchanged. No SDK source/internal API or production plugin behavior is
modified. This test-server configuration does not claim to test production server sticky timeout
behavior. In-process and subprocess test/demo workers use the same configuration.

The SDK also completes some shutdown callbacks asynchronously after `awaitTermination` returns.
The harness performs bounded checks of `isTerminated()` before releasing loaders. It separately
checks queue-named live threads and child-process exit; arbitrary application-created threads
whose names omit queue identity cannot be identified by the queue-name audit.

## Executed validation

The complete exact-SDK suite's XML reports contain these results:

| XML report | Passed | Failed/errors/skipped |
| --- | --- | --- |
| `build/test-results/test/TEST-io.temporal.worker.packages.acceptance.ClassLoaderIsolationTest.xml` | 1 | 0 |
| `build/test-results/test/TEST-io.temporal.worker.packages.acceptance.CrossProcessRetryTest.xml` | 1 | 0 |
| `build/test-results/test/TEST-io.temporal.worker.packages.acceptance.HeaderContractTest.xml` | 2 | 0 |
| `build/test-results/test/TEST-io.temporal.worker.packages.acceptance.HeaderDispatchIntegrationTest.xml` | 3 | 0 |
| `build/test-results/test/TEST-io.temporal.worker.packages.acceptance.PluginLifecycleIntegrationTest.xml` | 6 | 0 |
| `build/test-results/test/TEST-io.temporal.worker.packages.acceptance.PrivateJacksonIntegrationTest.xml` | 1 | 0 |
| `build/test-results/test/TEST-io.temporal.worker.packages.acceptance.VersionIsolationTest.xml` | 12 | 0 |
| `build/test-results/test/TEST-io.temporal.worker.packages.acceptance.VersionSelectorIntegrationTest.xml` | 2 | 0 |
| `build/test-results/test/TEST-io.temporal.worker.packages.acceptance.PatchDispatchIntegrationTest.xml` | 3 | 0 |

The HTML report is [build/reports/tests/test/index.html](../../build/reports/tests/test/index.html).
Expected infrastructure, business-failure, and malformed-header diagnostics appear in captured
logs because the tests deliberately exercise those errors; their XML results are passing.

For a focused integration run from the repository root:

```bash
./gradlew test -PtestJavaVersion=8 --tests '*VersionSelectorIntegrationTest' --tests '*PatchDispatchIntegrationTest'
```

All runtime, tooling and integration XML reports now share `build/test-results/test/`; the packaging plugin reports are under `build-logic/build/test-results/test/`. Historical validation counts above refer to the last completed full run. See the aggregate report for the latest validation after repository changes. Workflow timeouts retain histories and descriptions under ignored `build/diagnostics/`.
