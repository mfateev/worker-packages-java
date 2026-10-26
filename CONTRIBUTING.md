# Contributing

This independent Java library follows the conventions of sdk-java main. Build with JDK 21 (the SDK includes Java 17/21 multi-release implementations); library sources, tests and samples compile with `--release 8`. Running the worker requires Java 8 or later.

```bash
./scripts/bootstrap-sdk.sh
./gradlew spotlessApply
./gradlew clean build packageExamples installDist writeDependencyReport
./gradlew test -PtestJavaVersion=8
./gradlew demo -PtestJavaVersion=8
./scripts/validate-cli.py
```

The worker example follows official Temporal samples by using `ClientConfigProfile.load()` from the same SDK main checkout’s `temporal-envconfig` module to configure both service stubs and the client. Use `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_PROFILE`, and `TEMPORAL_CONFIG_FILE` for its connection settings.

The `testJavaVersion` property selects the library test and demo JDK using Gradle toolchains. Gradle plugin functional tests run on the build JDK 21 because they launch Gradle itself. Install the corresponding JDK locally; downloading JDKs is not implicit. Build Gradle itself on JDK 21. To run the installed CLI with another JDK, set `JAVA_HOME` to that JDK before invoking it or the CLI smoke script.

- Use Java 8 syntax and APIs in production code, tests, and application fixtures.
- Use JUnit 4.13.2, public test classes/methods, `TemporaryFolder`, and bounded `Timeout` rules. Preserve test coverage when changing harnesses.
- Format Java using Spotless and Google Java Format 1.24.0. As in the SDK, `classes` runs `spotlessApply`; `spotlessCheck` remains available for CI.
- Compiler flags match the SDK: UTF-8, `-parameters`, `-Xlint:none`, `-Xlint:deprecation`, and `-Werror`. Error Prone 2.31.0 checks handwritten Java.
- Keep public constructor and accessor signatures stable. Add focused tests for bug fixes and update documentation for user-facing changes.
- Write complete sentences in comments. Do not import SDK internal APIs or modify its source files.
- Run `build` before submitting changes. The included `build-logic` build supplies the workflow packaging plugin and its Gradle TestKit functional tests; root `check` also runs its checks. Root `build` includes tests, formatting validation, Javadoc/source JAR generation, and Java 8 bytecode validation for the distribution. Dependency locks must remain reproducible; regenerate intentionally with `--write-locks` when dependencies change.

Versioning follows SDK tag semantics: an exact `vX.Y.Z` or `vX.Y.Z-RCN` tag yields that version; commits after a final tag yield the next minor snapshot; commits after an RC yield that release's snapshot. A new repository without tags uses `0.1.0-SNAPSHOT`. `-PworkerPackagesReleaseVersion=X.Y.Z` (or `X.Y.Z-RCN`) is the validated release override. This property is namespaced because Gradle forwards `-P` properties to included builds: `releaseVersion` would also relabel the included SDK. The unscoped flag is rejected. SDK dependency versions live in `gradle.properties`; compilation uses the local main-branch source checkout and Maven publications keep the declared release version without a release compatibility check. Package versions in `.twp` manifests are independent string identifiers, including Git SHAs. A host version-selector callback chooses the implementation for each new workflow; there is no numeric maximum policy.

Local Maven publications include sources and Javadoc. No remote publishing destinations, credentials, or release automation are configured. The two production publications are `io.temporal:temporal-worker-packages` and `io.temporal:temporal-worker-packages-gradle-plugin`. Java packages use the `io.temporal.worker.packages` namespace. Unlike the SDK's timestamped manifest, JAR metadata omits wall-clock build times to preserve deterministic archives. Acceptance-test forks stay serial because each creates services and worker subprocesses.

A pull request should explain what changed, why, breaking changes, and any required server changes. See `ACCEPTANCE_REPORT.md` for recorded validation and test-server limitations.
