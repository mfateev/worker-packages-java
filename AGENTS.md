# Temporal Worker Packages contributor rules

Follow [CONTRIBUTING.md](CONTRIBUTING.md) and sdk-java conventions.

- Production code, tests and samples must compile against Java 8 APIs and bytecode.
- Preserve public constructors/accessors. Use JUnit 4 for tests.
- Format with `./gradlew spotlessApply`; validate with `./gradlew build` and relevant runtime tests.
- Keep UTF-8, compiler warnings-as-errors, Error Prone, dependency locks and strict archive validation enabled.
- Use complete sentences in comments and document user-facing changes.
- Use public SDK APIs and compile against the local sdk-java main checkout. Declare Maven versions in Gradle; do not add SDK SHA pins or runtime SDK revision checks.
- Do not add remote publication or release infrastructure without a requested release workflow.

The root Java library contains API, tooling, runtime and CLI packages in `src/main/java`. Keep tests in `src/test/java` and fixtures in `src/test/fixtures`. The only other production component is the packaging Gradle plugin in `build-logic/`.
