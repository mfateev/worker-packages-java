package io.temporal.worker.packages.acceptance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** Copies the archives built by each example's Gradle packaging task into test installations. */
public final class ExamplePackages {
  public static final String PACKAGE_ID = "greetings";
  public static final String WORKFLOW_TYPE = "GreetingWorkflow";

  private ExamplePackages() {}

  public static Path copy(Path target, int version) throws IOException {
    if (version != 1 && version != 2)
      throw new IllegalArgumentException("Example version must be 1 or 2");
    String name = "greetings-" + version + ".twp";
    Path source = FixtureCompiler.root().resolve("build/examples").resolve(name);
    Files.createDirectories(target);
    return Files.copy(source, target.resolve(name), StandardCopyOption.REPLACE_EXISTING);
  }
}
