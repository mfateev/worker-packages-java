# Acceptance fixtures

These sources deliberately stress isolation, payload conversion, replay, handler behavior,
activity retries/cancellation, lifecycle cleanup, and unsupported operations. They are test inputs;
runnable examples live in the separate [samples repository](https://github.com/mfateev/worker-packages-java-samples/blob/main/README.md).

Both orders variants export the same workflow, activity, and DTO class names. Their private
`example.rules.Rules` APIs conflict: version 2 uses a static `(String,int)` method and version 12
uses an instance `(int,String)` method. The rules JARs also contain conflicting resources and
service providers. Additional workflows exercise the acceptance matrix without complicating the
user-facing example.

The SDK, worker-package API, metrics API, and Jackson annotations are compile-only. Application
archives contain the application JAR and that version's rules JAR. The private Jackson test adds
isolated core/databind JARs while keeping annotations shared.

From the repository root, after SDK bootstrap:

```bash
./gradlew jar
./gradlew -p src/test/fixtures/orders-v2 build
./gradlew -p src/test/fixtures/orders-v12 build
./gradlew -p src/test/fixtures/rules-v2 jar
./gradlew -p src/test/fixtures/rules-v12 jar
```

Use `-PsdkDir=/absolute/path/to/sdk-java` for a nondefault checkout. The Maven dependency version is declared in the root `gradle.properties`; SDK main sources are substituted by Gradle.

`FixtureCompiler` independently compiles these same sources targeting Java 8, creates deterministic
JARs, and passes their paths to the archive packager. Fixture classes never enter the host/test
classpath; `Class.forName("example.orders.OrderWorkflowImpl")` must fail in the host loader.
The `FixtureCompiler` main class remains available to generate stress-test archives explicitly.
The separate samples repository uses its own Gradle-built greeting packages for its integration tests and restart demonstration.
