package io.temporal.worker.packages.tooling;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TypeCatalogTest {
  @Rule public TemporaryFolder temporary = new TemporaryFolder();
  @Rule public org.junit.rules.Timeout timeout = org.junit.rules.Timeout.seconds(60);

  @Test
  public void disjointArchivesMayShareVersionAndArchiveName() throws Exception {
    Path packages = temporary.newFolder("packages").toPath();
    pack(packages.resolve("one.twp"), "same-name", "sha", "One", "First");
    pack(packages.resolve("two.twp"), "same-name", "sha", "Two", "Second");
    PackageCatalog catalog = PackageCatalog.scan(packages);
    assertEquals(2, catalog.entries().size());
    assertEquals("sha", catalog.currentVersion());
    assertNotEquals(
        catalog.referenceForWorkflowType("One", "sha"),
        catalog.referenceForWorkflowType("Two", "sha"));
    assertNotEquals(
        catalog.referenceForActivityType("First", "sha"),
        catalog.referenceForActivityType("Second", "sha"));
    for (ValidatedPackage archive : catalog.entries())
      assertEquals(archive, catalog.require(archive.ref()));
  }

  @Test
  public void rejectsOnlyOverlappingTypesWithinEachVersionAndTypeNamespace() throws Exception {
    for (boolean workflow : new boolean[] {true, false}) {
      Path packages = temporary.newFolder().toPath();
      pack(packages.resolve("one.twp"), "one", "v1", "One", "First");
      pack(
          packages.resolve("two.twp"),
          "two",
          "v1",
          workflow ? "One" : "Two",
          workflow ? "Second" : "First");
      String message =
          assertThrows(PackageException.class, () -> PackageCatalog.scan(packages)).getMessage();
      assertTrue(
          message,
          message.contains(
              workflow ? "Duplicate workflow type One" : "Duplicate activity type First"));
      assertTrue(message, message.contains("one.twp") && message.contains("two.twp"));
    }
    Path packages = temporary.newFolder().toPath();
    pack(packages.resolve("one.twp"), "one", "v1", "Shared", "Activity");
    pack(packages.resolve("two.twp"), "two", "v1", "Other", "Shared");
    pack(packages.resolve("three.twp"), "three", "v2", "Shared", "Activity");
    assertEquals(3, PackageCatalog.scan(packages).entries().size());
  }

  @Test
  public void disjointArchivesCanBePatchedIndependentlyIncludingSharedPatchVersions()
      throws Exception {
    Path packages = temporary.newFolder().toPath();
    pack(packages.resolve("one.twp"), "one", "base", "One", "First");
    pack(packages.resolve("two.twp"), "two", "base", "Two", "Second");
    PackageCatalog original = PackageCatalog.scan(packages);
    io.temporal.worker.packages.api.PackageRef one =
        original.referenceForWorkflowType("One", "base");
    io.temporal.worker.packages.api.PackageRef two =
        original.referenceForActivityType("Second", "base");
    pack(packages.resolve("one-fix-base-patch.twp"), "one-fix", "patch", "One", "First", "base");
    pack(packages.resolve("two-fix-base-patch.twp"), "two-fix", "patch", "Two", "Second", "base");
    pack(packages.resolve("one-leaf-patch-leaf.twp"), "one-leaf", "leaf", "One", "First", "patch");
    PackageCatalog patched = PackageCatalog.scan(packages);
    assertEquals("leaf", patched.resolveForExecution(one, "One", true).ref().packageVersion());
    assertEquals("patch", patched.resolveForExecution(two, "Second", false).ref().packageVersion());
    Files.delete(packages.resolve("one.twp"));
    Files.delete(packages.resolve("two.twp"));
    PackageCatalog patchOnly = PackageCatalog.scan(packages);
    assertEquals("leaf", patchOnly.resolveForExecution(one, "One", true).ref().packageVersion());
    assertEquals(
        "patch", patchOnly.resolveForExecution(two, "Second", false).ref().packageVersion());
  }

  private void pack(Path output, String name, String version, String workflow, String activity)
      throws Exception {
    pack(output, name, version, workflow, activity, null);
  }

  private void pack(
      Path output, String name, String version, String workflow, String activity, String target)
      throws Exception {
    Path input = temporary.newFolder().toPath();
    Path jar = Files.createDirectories(input.resolve("app")).resolve("application.jar");
    try (JarOutputStream stream = new JarOutputStream(Files.newOutputStream(jar))) {
      stream.putNextEntry(new JarEntry("example/Application.class"));
      stream.write(new byte[] {1, 2, 3});
      stream.closeEntry();
    }
    PackageManifest manifest =
        new PackageManifest(
            1,
            1,
            name,
            version,
            8,
            "temporal-default-v1",
            Collections.singletonList("app/application.jar"),
            Collections.singletonList(
                new PackageManifest.Entrypoint(workflow, "example.Application")),
            Collections.singletonList(
                new PackageManifest.Entrypoint(activity, "example.Application")),
            target);
    Path metadata = input.resolve("workflow-package.json");
    Files.write(metadata, new ManifestCodec().write(manifest));
    new PackagePackager().pack(metadata, input, output);
  }
}
