package io.temporal.worker.packages.tooling;

import static org.junit.Assert.*;

import io.temporal.worker.packages.api.PackageRef;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PatchPackageToolingTest {
  @Rule public final TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void optionalPatchMetadataRoundTripsAndRejectsMalformedOrSelfTargets() {
    ManifestCodec codec = new ManifestCodec();
    PackageManifest normal = manifest("orders", "fix-1", null);
    String json = new String(codec.write(normal), StandardCharsets.UTF_8);
    assertFalse(json.contains("patchesVersion"));
    assertEquals("orders-fix-1.twp", normal.archiveFileName());
    PackageManifest patch = manifest("orders", "fix-1", "base");
    assertEquals("orders-base-fix-1.twp", patch.archiveFileName());
    assertEquals(patch, codec.read(codec.write(patch)));
    assertNotEquals(normal, patch);
    for (String value :
        Arrays.asList("null", "1", "true", "{}", "[]", "\"\"", "\"../base\"", "\"fix-1\"")) {
      String invalid = json.replaceFirst("\\{", "{\"patchesVersion\":" + value + ",");
      assertThrows(
          value,
          PackageException.class,
          () -> codec.read(invalid.getBytes(StandardCharsets.UTF_8)));
    }
    assertThrows(IllegalArgumentException.class, () -> manifest("orders", "fix-1", "fix-1"));
  }

  @Test
  public void patchOnlyDeploymentResolvesOldPinsAliasesAndStablePreparedCache() throws IOException {
    Path packages = temporary.newFolder("patch-only").toPath();
    ValidatedPackage patch = pack("orders", "fix-1", "base", packages);
    PackageCatalog catalog = PackageCatalog.scan(packages);
    PackageRef historical = new PackageRef("orders", "base", repeat('a', 64));
    assertEquals(patch, catalog.resolveForExecution(historical));
    assertThrows(PackageException.class, () -> catalog.require(historical));
    PackageRef alias = catalog.referenceForWorkflow("orders", "base");
    assertEquals(new PackageRef("orders", "base", patch.ref().sha256()), alias);
    assertEquals(patch, catalog.resolveForExecution(alias));
    assertEquals(patch.ref(), catalog.referenceForWorkflow("orders", "fix-1"));
    assertEquals(Arrays.asList("base", "fix-1"), catalog.selectionVersions());
    assertEquals(Collections.singletonList("fix-1"), catalog.versions("orders"));
    assertThrows(PackageException.class, () -> catalog.referenceForWorkflow("orders", "absent"));
    assertThrows(
        PackageException.class,
        () -> catalog.resolveForExecution(new PackageRef("other", "base", historical.sha256())));
    PackagePreparer preparer = new PackagePreparer(temporary.newFolder("cache").toPath());
    PreparedPackage first = preparer.prepare(patch);
    assertEquals(first, preparer.prepare(patch));
    assertEquals(patch.manifest(), first.manifest());
    assertTrue(Files.isRegularFile(first.classpath().get(0)));
  }

  @Test
  public void patchOverridesOldChecksumWhileUnpatchedPinsKeepStrictChecks() throws IOException {
    Path packages = temporary.newFolder("installed-base").toPath();
    ValidatedPackage base = pack("orders", "base", null, packages);
    ValidatedPackage patch = pack("orders", "fix-1", "base", packages);
    ValidatedPackage unrelated = pack("orders", "unrelated", null, packages);
    PackageCatalog catalog = PackageCatalog.scan(packages);
    PackageRef mismatchedBase = new PackageRef("orders", "base", repeat('0', 64));
    assertEquals(patch, catalog.resolveForExecution(base.ref()));
    assertEquals(patch, catalog.resolveForExecution(mismatchedBase));
    assertEquals(base.ref(), catalog.referenceForWorkflow("orders", "base"));
    assertEquals(base, catalog.require(base.ref()));
    assertThrows(PackageException.class, () -> catalog.require(mismatchedBase));
    assertEquals(unrelated, catalog.resolveForExecution(unrelated.ref()));
    assertThrows(
        PackageException.class,
        () -> catalog.resolveForExecution(new PackageRef("orders", "unrelated", repeat('0', 64))));
    assertThrows(
        PackageException.class,
        () -> catalog.resolveForExecution(new PackageRef("orders", "fix-1", repeat('0', 64))));
  }

  @Test
  public void chainedPatchesResolveOldAndIntermediatePinsToLeafWithoutBase() throws IOException {
    Path packages = temporary.newFolder("chain").toPath();
    ValidatedPackage first = pack("orders", "patch-1", "base", packages);
    ValidatedPackage leaf = pack("orders", "patch-2", "patch-1", packages);
    PackageCatalog catalog = PackageCatalog.scan(packages);
    assertEquals(
        leaf, catalog.resolveForExecution(new PackageRef("orders", "base", repeat('a', 64))));
    assertEquals(leaf, catalog.resolveForExecution(first.ref()));
    assertEquals(
        new PackageRef("orders", "base", leaf.ref().sha256()),
        catalog.referenceForWorkflow("orders", "base"));
    assertEquals(first.ref(), catalog.referenceForWorkflow("orders", "patch-1"));
    assertEquals(leaf.ref(), catalog.referenceForWorkflow("orders", "patch-2"));
    assertEquals(Arrays.asList("base", "patch-1", "patch-2"), catalog.selectionVersions());
    Path missingIntermediate = temporary.newFolder("missing-intermediate").toPath();
    Files.copy(leaf.archive(), missingIntermediate.resolve(leaf.manifest().archiveFileName()));
    PackageCatalog onlyLeaf = PackageCatalog.scan(missingIntermediate);
    assertThrows(PackageException.class, () -> onlyLeaf.referenceForWorkflow("orders", "base"));
    assertEquals(
        leaf.ref(),
        onlyLeaf.resolveForExecution(new PackageRef("orders", "patch-1", repeat('a', 64))).ref());
  }

  @Test
  public void catalogRejectsConflictingTypesAndCyclesButAllowsPatchesAcrossArchiveNames()
      throws IOException {
    Path conflicting = temporary.newFolder("conflicting").toPath();
    pack("orders", "fix-1", "base", conflicting);
    pack("orders", "fix-2", "base", conflicting);
    assertTrue(
        assertThrows(PackageException.class, () -> PackageCatalog.scan(conflicting))
            .getMessage()
            .contains("Multiple patches"));
    Path cycle = temporary.newFolder("cycle").toPath();
    pack("orders", "fix-1", "fix-2", cycle);
    pack("orders", "fix-2", "fix-1", cycle);
    assertTrue(
        assertThrows(PackageException.class, () -> PackageCatalog.scan(cycle))
            .getMessage()
            .contains("Patch cycle"));
    Path crossPackage = temporary.newFolder("cross-package").toPath();
    pack("billing", "base", null, crossPackage);
    pack("orders", "fix-1", "base", crossPackage);
    PackageCatalog cross = PackageCatalog.scan(crossPackage);
    assertEquals(
        "orders",
        cross
            .resolveForExecution(cross.require("billing", "base").ref(), "Workflow", true)
            .ref()
            .packageId());
    Path duplicate = temporary.newFolder("duplicate-global-version").toPath();
    pack("orders", "same", null, duplicate);
    pack("billing", "same", "base", duplicate);
    assertTrue(
        assertThrows(PackageException.class, () -> PackageCatalog.scan(duplicate))
            .getMessage()
            .contains("Duplicate workflow type"));
  }

  @Test
  public void patchNamesAreEnforcedForPackValidationAndCatalogButNotPrivateSnapshots()
      throws IOException {
    Path packages = temporary.newFolder("names").toPath();
    PackageManifest patchManifest = manifest("orders", "fix-1", "base");
    ValidatedPackage patch = pack("orders", "fix-1", "base", packages);
    Path renamedDirectory = temporary.newFolder("renamed").toPath();
    Path renamed = renamedDirectory.resolve("orders-fix-1.twp");
    Files.copy(patch.archive(), renamed);
    assertTrue(
        assertThrows(PackageException.class, () -> new PackageValidator().validate(renamed))
            .getMessage()
            .contains("orders-base-fix-1.twp"));
    assertThrows(PackageException.class, () -> PackageCatalog.scan(renamedDirectory));
    assertEquals(patch.manifest(), new PackageValidator().validate(renamed, false).manifest());
    Path badOutput = packages.resolve("wrong.twp");
    assertThrows(PackageException.class, () -> writePackage(patchManifest, badOutput));
    assertFalse(Files.exists(badOutput));
    Path arbitrary = packages.resolve("arbitrary-normal-name.twp");
    assertEquals(
        "normal",
        writePackage(manifest("orders", "normal", null), arbitrary).ref().packageVersion());
  }

  private ValidatedPackage pack(String id, String version, String target, Path directory)
      throws IOException {
    PackageManifest manifest = manifest(id, version, target);
    return writePackage(manifest, directory.resolve(manifest.archiveFileName()));
  }

  private ValidatedPackage writePackage(PackageManifest manifest, Path output) throws IOException {
    Path input = Files.createTempDirectory(temporary.getRoot().toPath(), "package-input-");
    Path app = Files.createDirectories(input.resolve("app")).resolve("workflow.jar");
    try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(app))) {
      JarEntry entry = new JarEntry("example/Workflow.class");
      entry.setTime(0);
      jar.putNextEntry(entry);
      jar.write(new byte[] {1, 2, 3});
      jar.closeEntry();
    }
    Path metadata = input.resolve("workflow-package.json");
    Files.write(metadata, new ManifestCodec().write(manifest));
    return new PackagePackager().pack(metadata, input, output);
  }

  private static PackageManifest manifest(String id, String version, String target) {
    return new PackageManifest(
        1,
        1,
        id,
        version,
        8,
        "temporal-default-v1",
        Collections.singletonList("app/workflow.jar"),
        Collections.singletonList(new PackageManifest.Entrypoint("Workflow", "example.Workflow")),
        Collections.emptyList(),
        target);
  }

  private static String repeat(char value, int count) {
    char[] result = new char[count];
    Arrays.fill(result, value);
    return new String(result);
  }
}
