package io.temporal.worker.packages.gradle;

import static org.junit.Assert.*;

import io.temporal.worker.packages.tooling.PackageManifest;
import io.temporal.worker.packages.tooling.PackageValidator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

public class WorkerPackagePluginTest {
  @Rule public final TemporaryFolder temporary = new TemporaryFolder();
  @Rule public final Timeout timeout = Timeout.seconds(180);

  @Test
  public void assemblePackagesPrivateRuntimeOnlyReproducibly() throws IOException {
    Path project = fixture("version = '7'\n");
    assertEquals(
        TaskOutcome.SUCCESS, run(project, false, "assemble").task(":workerPackage").getOutcome());
    Path archive = project.resolve("build/libs/greetings-7.twp");
    PackageManifest manifest = new PackageValidator().validate(archive).manifest();
    assertEquals("7", manifest.packageVersion());
    assertNull(manifest.patchesVersion());
    assertEquals(Arrays.asList("app/fixture-7.jar", "lib/private.jar"), manifest.classpath());
    assertEquals("GreetingWorkflow", manifest.workflows().get(0).type());
    assertEquals("Greet", manifest.activities().get(0).type());
    byte[] original = Files.readAllBytes(archive);
    assertEquals(
        TaskOutcome.UP_TO_DATE,
        run(project, false, "assemble").task(":workerPackage").getOutcome());
    run(project, false, "clean", "assemble");
    assertArrayEquals(original, Files.readAllBytes(archive));
  }

  @Test
  public void projectVersionDefaultsToSemanticVersion() throws IOException {
    Path project = fixture("version = '1.2.3'\n");
    run(project, false, "workerPackage");
    assertEquals(
        "1.2.3",
        new PackageValidator()
            .validate(project.resolve("build/libs/greetings-1.2.3.twp"))
            .manifest()
            .packageVersion());
  }

  @Test
  public void projectVersionDefaultsToGitSha() throws IOException {
    String sha = "adf0fc802850d5a35fc5ea34de8c9afbfddb6d6f";
    Path project = fixture("version = '" + sha + "'\n");
    run(project, false, "workerPackage");
    assertEquals(
        sha,
        new PackageValidator()
            .validate(project.resolve("build/libs/greetings-" + sha + ".twp"))
            .manifest()
            .packageVersion());
  }

  @Test
  public void explicitPackageVersionSupportsGitSha() throws IOException {
    String sha = "adf0fc802850d5a35fc5ea34de8c9afbfddb6d6f";
    Path project = fixture("version = '1.2.3'\nworkerPackage.packageVersion = '" + sha + "'\n");
    run(project, false, "workerPackage");
    assertEquals(
        sha,
        new PackageValidator()
            .validate(project.resolve("build/libs/greetings-" + sha + ".twp"))
            .manifest()
            .packageVersion());
  }

  @Test
  public void patchPackagePreservesRuntimeDependenciesAndNamesItsTarget() throws IOException {
    String target = "adf0fc802850d5a35fc5ea34de8c9afbfddb6d6f";
    String patch = "b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0";
    Path project =
        fixture("version = '" + patch + "'\nworkerPackage.patchesVersion = '" + target + "'\n");
    assertEquals(
        TaskOutcome.SUCCESS, run(project, false, "assemble").task(":workerPackage").getOutcome());
    Path archive = project.resolve("build/libs/greetings-" + target + "-" + patch + ".twp");
    PackageManifest manifest = new PackageValidator().validate(archive).manifest();
    assertEquals(patch, manifest.packageVersion());
    assertEquals(target, manifest.patchesVersion());
    assertEquals(
        Arrays.asList("app/fixture-" + patch + ".jar", "lib/private.jar"), manifest.classpath());
    byte[] original = Files.readAllBytes(archive);
    assertEquals(
        TaskOutcome.UP_TO_DATE,
        run(project, false, "assemble").task(":workerPackage").getOutcome());
    run(project, false, "clean", "assemble");
    assertArrayEquals(original, Files.readAllBytes(archive));
  }

  @Test
  public void patchPackageSupportsSemanticVersionTokens() throws IOException {
    Path project = fixture("version = '1.2.4-hotfix'\nworkerPackage.patchesVersion = '1.2.3'\n");
    run(project, false, "workerPackage");
    PackageManifest manifest =
        new PackageValidator()
            .validate(project.resolve("build/libs/greetings-1.2.3-1.2.4-hotfix.twp"))
            .manifest();
    assertEquals("1.2.3", manifest.patchesVersion());
    assertEquals("1.2.4-hotfix", manifest.packageVersion());
  }

  @Test
  public void patchPackageCanTargetAnotherPatchVersion() throws IOException {
    Path project = fixture("version = 'hotfix-2'\nworkerPackage.patchesVersion = 'hotfix-1'\n");
    run(project, false, "workerPackage");
    PackageManifest manifest =
        new PackageValidator()
            .validate(project.resolve("build/libs/greetings-hotfix-1-hotfix-2.twp"))
            .manifest();
    assertEquals("hotfix-1", manifest.patchesVersion());
    assertEquals("hotfix-2", manifest.packageVersion());
  }

  @Test
  public void rejectsBlankAndUnsafePatchTargets() throws IOException {
    for (String target : Arrays.asList("", "../escape", "with space")) {
      Path project =
          fixture("version = 'hotfix'\nworkerPackage.patchesVersion = '" + target + "'\n");
      assertTrue(
          run(project, true, "workerPackage").getOutput().contains("Invalid package version"));
    }
  }

  @Test
  public void rejectsPatchTargetingItsOwnVersion() throws IOException {
    Path project = fixture("version = 'hotfix'\nworkerPackage.patchesVersion = 'hotfix'\n");
    assertTrue(
        run(project, true, "workerPackage").getOutput().contains("cannot patch its own version"));
  }

  @Test
  public void rejectsMissingEntrypointsAndBlankDefaultVersion() throws IOException {
    Path project = fixture("version = '1'\nworkerPackage { workflows = [:]; activities = [:] }\n");
    assertTrue(
        run(project, true, "workerPackage")
            .getOutput()
            .contains("At least one workflow or activity is required"));
    Path versionProject = fixture("version = ''\n");
    assertTrue(
        run(versionProject, true, "workerPackage").getOutput().contains("nonblank version token"));
  }

  @Test
  public void rejectsUnsafeExplicitVersion() throws IOException {
    Path project = fixture("version = '1'\nworkerPackage.packageVersion = '../escape'\n");
    assertTrue(run(project, true, "workerPackage").getOutput().contains("Invalid package version"));
  }

  @Test
  public void rejectsRuntimeJarFilenameCollisions() throws IOException {
    Path project =
        fixture("version = '1'\ndependencies { runtimeOnly files('other/private.jar') }\n");
    jar(project.resolve("other/private.jar"), "another.txt");
    assertTrue(
        run(project, true, "workerPackage")
            .getOutput()
            .contains("Runtime JAR filename collision: private.jar"));
  }

  @Test
  public void packagesActivitiesWithoutWorkflows() throws IOException {
    Path project = fixture("version = '1'\nworkerPackage { workflows = [:] }\n");
    run(project, false, "workerPackage");
    PackageManifest manifest =
        new PackageValidator().validate(project.resolve("build/libs/greetings-1.twp")).manifest();
    assertEquals(0, manifest.workflows().size());
    assertEquals(1, manifest.activities().size());
  }

  private Path fixture(String extra) throws IOException {
    Path project = temporary.newFolder().toPath();
    write(project.resolve("settings.gradle"), "rootProject.name = 'fixture'\n");
    write(
        project.resolve("build.gradle"),
        "plugins { id 'io.temporal.worker-packages' }\n"
            + "java { sourceCompatibility = JavaVersion.VERSION_1_8; targetCompatibility = JavaVersion.VERSION_1_8 }\n"
            + "dependencies { implementation files('deps/private.jar'); compileOnly files('deps/sdk.jar') }\n"
            + "workerPackage {\n"
            + "  packageId = 'greetings'\n"
            + "  workflows = [GreetingWorkflow: 'example.GreetingWorkflowImpl']\n"
            + "  activities = [Greet: 'example.GreetingActivitiesImpl']\n"
            + "}\n"
            + extra);
    write(
        project.resolve("src/main/java/example/GreetingWorkflowImpl.java"),
        "package example; public class GreetingWorkflowImpl {}\n");
    jar(project.resolve("deps/private.jar"), "private-resource.txt");
    jar(project.resolve("deps/sdk.jar"), "io/temporal/Forbidden.class");
    return project;
  }

  private static BuildResult run(Path project, boolean failure, String... tasks) {
    java.util.List<String> arguments = new java.util.ArrayList<>(Arrays.asList(tasks));
    arguments.add("--stacktrace");
    arguments.add("--no-build-cache");
    GradleRunner runner =
        GradleRunner.create()
            .withProjectDir(project.toFile())
            .withPluginClasspath()
            .withArguments(arguments);
    return failure ? runner.buildAndFail() : runner.build();
  }

  private static void write(Path path, String text) throws IOException {
    Files.createDirectories(path.getParent());
    Files.write(path, text.getBytes(StandardCharsets.UTF_8));
  }

  private static void jar(Path path, String entry) throws IOException {
    Files.createDirectories(path.getParent());
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(path))) {
      ZipEntry item = new ZipEntry(entry);
      item.setTime(0);
      out.putNextEntry(item);
      out.write("fixture".getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
  }
}
