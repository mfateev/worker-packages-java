package io.temporal.worker.packages.tooling;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.worker.packages.api.PackageRef;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.jar.*;
import java.util.zip.*;
import javax.tools.ToolProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PackageToolingTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private Path temp;

  @Before
  public void setUp() {
    temp = temporaryFolder.getRoot().toPath();
  }

  @Test
  public void manifestRejectsCoercionUnknownMissingDuplicateAndInvalidFields() throws Exception {
    String json =
        new String(new ManifestCodec().write(manifest("orders", 2)), StandardCharsets.UTF_8);
    for (String value :
        java.util.Arrays.asList(
            "0",
            "-1",
            "2",
            "2.0",
            "2147483648",
            "true",
            "null",
            "{}",
            "[]",
            "\"\"",
            "\"../escape\"",
            "\"contains space\"",
            "\"-prefix\"",
            "\"" + TestSupport.repeat("a", 129) + "\"")) {
      assertThrows(
          value,
          PackageException.class,
          () ->
              new ManifestCodec()
                  .read(
                      json.replace("\"packageVersion\" : \"2\"", "\"packageVersion\" : " + value)
                          .getBytes(StandardCharsets.UTF_8)));
    }
    for (String invalid :
        java.util.Arrays.asList(
            json.replaceFirst("\\{", "{\"unknown\": 3,"),
            json.replaceFirst("\\{", "{\"packageId\":\"other\","),
            json.replace("\"packageId\" : \"orders\",", ""),
            json.replace("\"orders\"", "\"../orders\""),
            json.replace("\"javaRelease\" : 8", "\"javaRelease\" : 17"),
            json.replace("temporal-default-v1", "custom"),
            json + " {}")) {
      assertThrows(
          invalid,
          PackageException.class,
          () -> new ManifestCodec().read(invalid.getBytes(StandardCharsets.UTF_8)));
    }
    assertEquals(
        manifest("orders", 2), new ManifestCodec().read(json.getBytes(StandardCharsets.UTF_8)));
    for (String version :
        Arrays.asList("0", "1.2.3", "1.2.3-rc.1", "adf0fc802850d5a35fc5ea34de8c9afbfddb6d6f")) {
      PackageManifest expected = manifest("orders", version);
      assertEquals(
          version, new ManifestCodec().read(new ManifestCodec().write(expected)).packageVersion());
    }
    String legacy =
        json.replaceFirst("\\{", "{\"temporalSdkRevision\":\"unrelated-old-sdk-revision\",");
    PackageManifest legacyManifest =
        new ManifestCodec().read(legacy.getBytes(StandardCharsets.UTF_8));
    assertEquals(manifest("orders", 2), legacyManifest);
    assertFalse(
        new String(new ManifestCodec().write(legacyManifest), StandardCharsets.UTF_8)
            .contains("temporalSdkRevision"));
  }

  @Test
  public void manifestRejectsDuplicateTypesInvalidBinaryNamesAndClasspath() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    for (String path :
        java.util.Arrays.asList(
            "../app.jar", "/app.jar", "C:/app.jar", "a\\b.jar", "a/./b.jar", "a//b.jar", "a.txt")) {
      com.fasterxml.jackson.databind.JsonNode node = mapper.valueToTree(manifest("orders", 2));
      ((com.fasterxml.jackson.databind.node.ObjectNode) node).putArray("classpath").add(path);
      assertThrows(
          path,
          PackageException.class,
          () -> new ManifestCodec().read(mapper.writeValueAsBytes(node)));
    }
    com.fasterxml.jackson.databind.JsonNode node = mapper.valueToTree(manifest("orders", 2));
    ((com.fasterxml.jackson.databind.node.ObjectNode) node)
        .putArray("workflows")
        .add(mapper.valueToTree(new PackageManifest.Entrypoint("OrderWorkflow", "bad..Name")));
    byte[] invalidBinary = mapper.writeValueAsBytes(node);
    assertThrows(PackageException.class, () -> new ManifestCodec().read(invalidBinary));
    node = mapper.valueToTree(manifest("orders", 2));
    ((com.fasterxml.jackson.databind.node.ArrayNode) node.get("workflows"))
        .add(node.get("workflows").get(0));
    byte[] duplicate = mapper.writeValueAsBytes(node);
    assertThrows(PackageException.class, () -> new ManifestCodec().read(duplicate));
  }

  @Test
  public void catalogListsVersionsLexicallyWithoutImplicitSelectionAndRejectsDuplicates()
      throws Exception {
    Path packages = Files.createDirectory(temp.resolve("packages"));
    pack("orders", 2, packages.resolve("z.twp"));
    pack("orders", 9, packages.resolve("a.twp"));
    pack("orders", 12, packages.resolve("m.twp"));
    pack("billing", 3, packages.resolve("billing.twp"));
    TestSupport.writeString(packages.resolve("ignored.txt"), "not an archive");
    Files.createDirectory(packages.resolve("directory.twp"));
    Files.createSymbolicLink(packages.resolve("ignored-link.twp"), packages.resolve("z.twp"));
    PackageCatalog catalog = PackageCatalog.scan(packages);
    assertThrows(PackageException.class, () -> catalog.currentVersion("orders"));
    String ambiguous = assertThrows(PackageException.class, catalog::currentVersion).getMessage();
    assertTrue(ambiguous.contains("specify a package version explicitly"));
    assertThrows(PackageException.class, () -> catalog.currentVersion("billing"));
    assertEquals(java.util.Arrays.asList("12", "2", "9"), catalog.versions("orders"));
    assertEquals("2", catalog.require("orders", 2).ref().packageVersion());
    assertEquals(catalog.require("orders", 2), catalog.require("orders", "2"));
    Path sole = Files.createDirectory(temp.resolve("sole-version"));
    String sha = "adf0fc802850d5a35fc5ea34de8c9afbfddb6d6f";
    pack("orders", sha, sole.resolve("sha.twp"));
    PackageCatalog only = PackageCatalog.scan(sole);
    assertEquals(sha, only.currentVersion());
    assertEquals(sha, only.currentVersion("orders"));
    assertEquals(sha, only.require("orders", sha).ref().packageVersion());
    assertEquals(
        java.util.Arrays.asList("billing", "orders"), TestSupport.copyList(catalog.packageIds()));
    assertEquals(4, catalog.entries().size());
    assertThrows(UnsupportedOperationException.class, () -> catalog.entries().clear());
    assertThrows(PackageException.class, () -> catalog.require("orders", 8));
    assertThrows(
        PackageException.class,
        () -> catalog.require(new PackageRef("orders", 2, TestSupport.repeat("0", 64))));
    Files.copy(packages.resolve("z.twp"), packages.resolve("copy.twp"));
    String message =
        assertThrows(PackageException.class, () -> PackageCatalog.scan(packages)).getMessage();
    assertTrue(message.contains("copy.twp") && message.contains("z.twp"));
    Path conflicting = Files.createDirectory(temp.resolve("conflicting-versions"));
    pack("orders", 2, conflicting.resolve("orders.twp"));
    pack("billing", 2, conflicting.resolve("billing.twp"));
    String conflict =
        assertThrows(PackageException.class, () -> PackageCatalog.scan(conflicting)).getMessage();
    assertTrue(conflict.contains("version 2"));
    assertTrue(conflict.contains("orders.twp") && conflict.contains("billing.twp"));
  }

  @Test
  public void emptyAndMissingDirectoriesHaveDifferentSemantics() throws Exception {
    assertTrue(PackageCatalog.scan(temp).isEmpty());
    assertThrows(PackageException.class, () -> PackageCatalog.scan(temp.resolve("absent")));
  }

  @Test
  public void deterministicPackagerPreservesJarBytes() throws Exception {
    Path inputs = inputs("orders", 2, TestSupport.map("example/Foo.class", new byte[] {1, 2, 3}));
    PackagePackager packager = new PackagePackager();
    io.temporal.worker.packages.tooling.ValidatedPackage first =
        packager.pack(inputs.resolve("workflow-package.json"), inputs, temp.resolve("one.twp"));
    io.temporal.worker.packages.tooling.ValidatedPackage second =
        packager.pack(inputs.resolve("workflow-package.json"), inputs, temp.resolve("two.twp"));
    assertArrayEquals(Files.readAllBytes(first.archive()), Files.readAllBytes(second.archive()));
    assertEquals(first.ref(), second.ref());
    try (ZipFile zip = new ZipFile(first.archive().toFile())) {
      assertArrayEquals(
          Files.readAllBytes(inputs.resolve("app/orders.jar")),
          TestSupport.readAllBytes(zip.getInputStream(zip.getEntry("app/orders.jar"))));
    }
  }

  @Test
  public void rejectsSharedClassesIncludingMultiReleaseAndDuplicateDefinitions() throws Exception {
    for (String name :
        java.util.Arrays.asList(
            "io/temporal/workflow/Workflow.class",
            "com/uber/m3/tally/Scope.class",
            "com/fasterxml/jackson/annotation/JsonProperty.class",
            "META-INF/versions/21/java/lang/String.class",
            "io/temporal/worker/packages/api/X.class",
            "io/nexusrpc/X.class",
            "com/google/common/util/concurrent/ListenableFuture.class")) {
      Path input = inputs("orders", 2, TestSupport.map(name, new byte[] {1}));
      assertThrows(
          name,
          PackageException.class,
          () ->
              new PackagePackager()
                  .pack(input.resolve("workflow-package.json"), input, temp.resolve("bad.twp")));
    }
    Path input =
        inputs(
            "orders",
            2,
            TestSupport.map(
                "example/X.class",
                new byte[] {1},
                "META-INF/versions/21/example/X.class",
                new byte[] {2},
                "module-info.class",
                new byte[] {1}));
    assertNotNull(
        new PackagePackager()
            .pack(input.resolve("workflow-package.json"), input, temp.resolve("mr.twp")));
    Path extra = input.resolve("lib/extra.jar");
    Files.createDirectories(extra.getParent());
    writeZip(extra, TestSupport.map("example/X.class", new byte[] {3}));
    PackageManifest m = manifest("orders", 2);
    PackageManifest dup =
        new PackageManifest(
            1,
            1,
            "orders",
            2,
            8,
            "temporal-default-v1",
            java.util.Arrays.asList("app/orders.jar", "lib/extra.jar"),
            m.workflows(),
            m.activities());
    Files.write(input.resolve("workflow-package.json"), new ManifestCodec().write(dup));
    assertTrue(
        assertThrows(
                PackageException.class,
                () ->
                    new PackagePackager()
                        .pack(
                            input.resolve("workflow-package.json"), input, temp.resolve("dup.twp")))
            .getMessage()
            .contains("Duplicate class"));
  }

  @Test
  public void rejectsManifestClasspathsAndIndexesWithoutRewriting() throws Exception {
    for (String metadata :
        java.util.Arrays.asList(
            "Manifest-Version: 1.0\r\nClass-Path: \r\n\r\n",
            "Manifest-Version: 1.0\r\nClass-Path: ../external.jar\r\n\r\n",
            "Manifest-Version: 1.0\r\nClass-Path: file:/tmp/external.jar\r\n\r\n",
            "Manifest-Version: 1.0\r\n\r\nName: foo\r\nClass-Path: app.jar\r\n\r\n")) {
      Path input =
          inputs(
              "orders",
              2,
              TestSupport.map("META-INF/MANIFEST.MF", metadata.getBytes(StandardCharsets.UTF_8)));
      byte[] before = Files.readAllBytes(input.resolve("app/orders.jar"));
      assertTrue(
          assertThrows(
                  PackageException.class,
                  () ->
                      new PackagePackager()
                          .pack(
                              input.resolve("workflow-package.json"),
                              input,
                              temp.resolve("bad.twp")))
              .getMessage()
              .contains("Class-Path"));
      assertArrayEquals(before, Files.readAllBytes(input.resolve("app/orders.jar")));
    }
    Path input = inputs("orders", 2, TestSupport.map("META-INF/INDEX.LIST", new byte[0]));
    assertThrows(
        PackageException.class,
        () ->
            new PackagePackager()
                .pack(input.resolve("workflow-package.json"), input, temp.resolve("bad.twp")));
  }

  @Test
  public void rejectsUnsafeNestedAndOuterPathsExtrasMissingAndDuplicateEntries() throws Exception {
    for (String path :
        java.util.Arrays.asList(
            "../escape.class", "/absolute.class", "a/../b.class", "a\\b.class")) {
      Path input = inputs("orders", 2, TestSupport.map(path, new byte[] {1}));
      assertThrows(
          path,
          PackageException.class,
          () ->
              new PackagePackager()
                  .pack(input.resolve("workflow-package.json"), input, temp.resolve("bad.twp")));
    }
    byte[] manifest = new ManifestCodec().write(manifest("orders", 2));
    byte[] jar = zipBytes(TestSupport.map("example/X.class", new byte[0]));
    for (Map<String, byte[]> contents :
        java.util.Arrays.<Map<String, byte[]>>asList(
            TestSupport.map("workflow-package.json", manifest),
            TestSupport.map(
                "workflow-package.json", manifest, "app/orders.jar", jar, "extra.txt", new byte[0]),
            TestSupport.map("workflow-package.json", manifest, "../escape.jar", jar))) {
      Path archive = temp.resolve(UUID.randomUUID() + ".twp");
      writeZip(archive, contents);
      assertThrows(PackageException.class, () -> new PackageValidator().validate(archive));
    }
    Path duplicate = temp.resolve("duplicate.twp");
    byte[] raw =
        zipBytes(
            new LinkedHashMap<>(TestSupport.map("one.txt", new byte[0], "two.txt", new byte[0])));
    String mapped = new String(raw, StandardCharsets.ISO_8859_1).replace("two.txt", "one.txt");
    Files.write(duplicate, mapped.getBytes(StandardCharsets.ISO_8859_1));
    assertTrue(
        assertThrows(PackageException.class, () -> new PackageValidator().validate(duplicate))
            .getMessage()
            .contains("Duplicate ZIP entry"));
  }

  @Test
  public void rejectsUnixSymlinksInOuterAndNestedZipsAndSourceJarSymlinks() throws Exception {
    byte[] symlink = zipBytes(TestSupport.map("link", "outside".getBytes(StandardCharsets.UTF_8)));
    for (int i = 0; i < symlink.length - 46; i++)
      if (symlink[i] == 0x50
          && symlink[i + 1] == 0x4b
          && symlink[i + 2] == 1
          && symlink[i + 3] == 2) {
        int mode = 0120777;
        symlink[i + 40] = (byte) mode;
        symlink[i + 41] = (byte) (mode >>> 8);
        break;
      }
    Path outer = temp.resolve("symlink.twp");
    Files.write(outer, symlink);
    assertTrue(
        assertThrows(PackageException.class, () -> new PackageValidator().validate(outer))
            .getMessage()
            .contains("Symbolic link"));
    Path input = inputs("orders", 2, TestSupport.map("example/X.class", new byte[0]));
    Files.write(input.resolve("app/orders.jar"), symlink);
    assertTrue(
        assertThrows(
                PackageException.class,
                () ->
                    new PackagePackager()
                        .pack(
                            input.resolve("workflow-package.json"),
                            input,
                            temp.resolve("nested.twp")))
            .getMessage()
            .contains("Symbolic link"));
    Path original = temp.resolve("original.jar");
    Files.move(input.resolve("app/orders.jar"), original);
    Files.createSymbolicLink(input.resolve("app/orders.jar"), original);
    assertThrows(
        PackageException.class,
        () ->
            new PackagePackager()
                .pack(input.resolve("workflow-package.json"), input, temp.resolve("source.twp")));
  }

  @Test
  public void enforcesStreamingCompressedExpandedManifestAndEntryLimits() throws Exception {
    io.temporal.worker.packages.tooling.ValidatedPackage archive =
        pack("orders", 2, temp.resolve("valid.twp"));
    for (ValidationLimits limits :
        java.util.Arrays.asList(
            new ValidationLimits(1, 1L << 20, 1L << 20, 100),
            new ValidationLimits(1L << 20, 8, 1L << 20, 100),
            new ValidationLimits(1L << 20, 1L << 20, 8, 100),
            new ValidationLimits(1L << 20, 1L << 20, 1L << 20, 1))) {
      assertThrows(
          PackageException.class, () -> new PackageValidator(limits).validate(archive.archive()));
    }
    Path input = inputs("orders", 2, TestSupport.map("bomb.txt", new byte[32 * 1024]));
    io.temporal.worker.packages.tooling.ValidatedPackage bomb =
        new PackagePackager()
            .pack(input.resolve("workflow-package.json"), input, temp.resolve("bomb.twp"));
    assertThrows(
        PackageException.class,
        () ->
            new PackageValidator(new ValidationLimits(1L << 20, 4096, 1L << 20, 100))
                .validate(bomb.archive()));
  }

  @Test
  public void validationDoesNotInitializeApplicationClasses() throws Exception {
    Path source = temp.resolve("Entrypoint.java");
    Path classes = Files.createDirectory(temp.resolve("classes"));
    Path marker = temp.resolve("initialized");
    TestSupport.writeString(
        source,
        "package example; public class Entrypoint {static {try {java.nio.file.Files.write(java.nio.file.Paths.get(\""
            + marker
            + "\"),\"initialized\".getBytes(java.nio.charset.StandardCharsets.UTF_8));}catch(Exception e){throw new RuntimeException(e);}}}");
    assertEquals(
        0,
        ToolProvider.getSystemJavaCompiler()
            .run(
                null,
                null,
                null,
                "-source",
                "8",
                "-target",
                "8",
                "-d",
                classes.toString(),
                source.toString()));
    Path input =
        inputs(
            "orders",
            2,
            TestSupport.map(
                "example/Entrypoint.class",
                Files.readAllBytes(classes.resolve("example/Entrypoint.class"))));
    new PackagePackager()
        .pack(input.resolve("workflow-package.json"), input, temp.resolve("lazy.twp"));
    assertFalse(Files.exists(marker));
  }

  @Test
  public void preparationReusesVerifiedContentAndRejectsCorruptionOrChangedSources()
      throws Exception {
    ValidatedPackage archive = pack("orders", 2, temp.resolve("valid.twp"));
    PackagePreparer preparer = new PackagePreparer(temp.resolve("cache"));
    PreparedPackage prepared = preparer.prepare(archive);
    assertEquals(prepared, preparer.prepare(archive));
    assertEquals(1, prepared.classpath().size());
    TestSupport.writeString(prepared.classpath().get(0), "corrupt");
    assertThrows(PackageException.class, () -> preparer.prepare(archive));
    TestSupport.writeString(archive.archive(), "changed");
    assertThrows(
        PackageException.class,
        () -> new PackagePreparer(temp.resolve("fresh-cache")).prepare(archive));
    try (java.util.stream.Stream<java.nio.file.Path> paths =
        Files.list(temp.resolve("fresh-cache"))) {
      assertFalse(paths.anyMatch(p -> p.getFileName().toString().startsWith(".preparing-")));
    }
  }

  @Test
  public void concurrentArchiveReplacementNeverPairsMetadataWithAnotherDigest() throws Exception {
    ValidatedPackage two = pack("orders", 2, temp.resolve("two.twp"));
    ValidatedPackage twelve = pack("orders", 12, temp.resolve("twelve.twp"));
    Map<String, PackageManifest> known =
        TestSupport.map(
            two.ref().sha256(), two.manifest(), twelve.ref().sha256(), twelve.manifest());
    Path changing = temp.resolve("changing.twp");
    Files.copy(two.archive(), changing);
    java.util.concurrent.atomic.AtomicBoolean stop =
        new java.util.concurrent.atomic.AtomicBoolean();
    java.util.concurrent.atomic.AtomicInteger replacements =
        new java.util.concurrent.atomic.AtomicInteger();
    CountDownLatch started = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> replacing =
        executor.submit(
            () -> {
              try {
                while (!stop.get()) {
                  Path incoming = Files.createTempFile(temp, "replacement-", ".twp");
                  Files.copy(
                      (replacements.get() % 2 == 0 ? twelve : two).archive(),
                      incoming,
                      StandardCopyOption.REPLACE_EXISTING);
                  Files.move(
                      incoming,
                      changing,
                      StandardCopyOption.ATOMIC_MOVE,
                      StandardCopyOption.REPLACE_EXISTING);
                  replacements.incrementAndGet();
                  started.countDown();
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    int validated = 0;
    try {
      assertTrue("Replacement process never started", started.await(10, TimeUnit.SECONDS));
      for (int attempt = 0; attempt < 100; attempt++) {
        ValidatedPackage result;
        try {
          result = new PackageValidator().validate(changing);
        } catch (PackageException changedDuringCopy) {
          continue;
        }
        assertEquals(changing.toAbsolutePath(), result.archive());
        assertEquals(
            "Archive replacement must not pair metadata with a different archive digest",
            known.get(result.ref().sha256()),
            result.manifest());
        assertEquals(result.manifest().packageVersion(), result.ref().packageVersion());
        validated++;
      }
      assertTrue("No complete archive was validated", validated > 0);
      assertTrue("Archive was not repeatedly replaced", replacements.get() > 1);
    } finally {
      stop.set(true);
      try {
        replacing.get(10, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  public void customLimitsAreRetainedDuringPreparation() throws Exception {
    byte[] json = new ManifestCodec().write(manifest("orders", 2));
    byte[] padded = new byte[(1 << 20) + json.length];
    Arrays.fill(padded, (byte) ' ');
    System.arraycopy(json, 0, padded, 1 << 20, json.length);
    Path archive = temp.resolve("large-manifest.twp");
    writeZip(
        archive,
        TestSupport.map(
            "workflow-package.json",
            padded,
            "app/orders.jar",
            zipBytes(TestSupport.map("example/X.class", new byte[0]))));
    assertThrows(PackageException.class, () -> new PackageValidator().validate(archive));
    ValidationLimits limits = new ValidationLimits(4L << 20, 8L << 20, 2L << 20, 100);
    ValidatedPackage validated = new PackageValidator(limits).validate(archive);
    assertEquals(limits, validated.limits());
    assertEquals(
        validated.ref(),
        new PackagePreparer(temp.resolve("custom-cache")).prepare(validated).ref());
  }

  @Test
  public void concurrentProcessesPublishOneCompletePreparedPackage() throws Exception {
    ValidatedPackage archive = pack("orders", 2, temp.resolve("valid.twp"));
    Path cache = temp.resolve("cache");
    String cp = subprocessClasspath();
    Process a =
        new ProcessBuilder(
                java(),
                "-cp",
                cp,
                CacheProcess.class.getName(),
                archive.archive().toString(),
                cache.toString())
            .redirectErrorStream(true)
            .start();
    Process b =
        new ProcessBuilder(
                java(),
                "-cp",
                cp,
                CacheProcess.class.getName(),
                archive.archive().toString(),
                cache.toString())
            .redirectErrorStream(true)
            .start();
    try {
      assertTrue(a.waitFor(30, TimeUnit.SECONDS));
      assertTrue(b.waitFor(30, TimeUnit.SECONDS));
      assertEquals(
          new String(TestSupport.readAllBytes(a.getInputStream()), StandardCharsets.UTF_8),
          0,
          a.exitValue());
      assertEquals(
          new String(TestSupport.readAllBytes(b.getInputStream()), StandardCharsets.UTF_8),
          0,
          b.exitValue());
      assertEquals(archive.ref(), new PackagePreparer(cache).prepare(archive).ref());
      try (java.util.stream.Stream<java.nio.file.Path> files = Files.list(cache)) {
        assertEquals(2, files.count());
      }
    } finally {
      a.destroyForcibly();
      b.destroyForcibly();
    }
  }

  public static class CacheProcess {
    public static void main(String[] args) {
      io.temporal.worker.packages.tooling.ValidatedPackage archive =
          new PackageValidator().validate(Paths.get(args[0]));
      io.temporal.worker.packages.tooling.PreparedPackage prepared =
          new PackagePreparer(Paths.get(args[1])).prepare(archive);
      if (!Files.isRegularFile(prepared.classpath().get(0)))
        throw new AssertionError("Partial extraction");
    }
  }

  private static String java() {
    return Paths.get(System.getProperty("java.home"), "bin", "java").toString();
  }

  private static String subprocessClasspath() throws Exception {
    Set<String> paths = new LinkedHashSet<>();
    for (Class<?> type :
        java.util.Arrays.asList(
            PackageToolingTest.class,
            PackageValidator.class,
            PackageRef.class,
            ObjectMapper.class,
            com.fasterxml.jackson.core.JsonFactory.class,
            com.fasterxml.jackson.annotation.JsonProperty.class)) {
      paths.add(
          Paths.get(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toString());
    }
    return String.join(File.pathSeparator, paths);
  }

  private ValidatedPackage pack(String id, int version, Path output) throws Exception {
    return pack(id, PackageRef.validateVersion(version), output);
  }

  private ValidatedPackage pack(String id, String version, Path output) throws Exception {
    Path input = inputs(id, version, TestSupport.map("example/Foo.class", new byte[] {1, 2}));
    return new PackagePackager().pack(input.resolve("workflow-package.json"), input, output);
  }

  private Path inputs(String id, int version, Map<String, byte[]> jarEntries) throws Exception {
    return inputs(id, PackageRef.validateVersion(version), jarEntries);
  }

  private Path inputs(String id, String version, Map<String, byte[]> jarEntries) throws Exception {
    Path input = Files.createTempDirectory(temp, "inputs-");
    Files.createDirectory(input.resolve("app"));
    Files.write(
        input.resolve("workflow-package.json"), new ManifestCodec().write(manifest(id, version)));
    writeZip(input.resolve("app/orders.jar"), jarEntries);
    return input;
  }

  private static PackageManifest manifest(String id, int version) {
    return manifest(id, PackageRef.validateVersion(version));
  }

  private static PackageManifest manifest(String id, String version) {
    return new PackageManifest(
        1,
        1,
        id,
        version,
        8,
        "temporal-default-v1",
        java.util.Arrays.asList("app/orders.jar"),
        java.util.Arrays.asList(
            new PackageManifest.Entrypoint("OrderWorkflow", "example.OrderWorkflowImpl")),
        java.util.Arrays.asList(
            new PackageManifest.Entrypoint("ChargeCard", "example.PaymentActivitiesImpl")));
  }

  private static byte[] zipBytes(Map<String, byte[]> entries) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
      for (java.util.Map.Entry<java.lang.String, byte[]> entry : entries.entrySet()) {
        zip.putNextEntry(new ZipEntry(entry.getKey()));
        zip.write(entry.getValue());
        zip.closeEntry();
      }
    }
    return bytes.toByteArray();
  }

  private static void writeZip(Path output, Map<String, byte[]> entries) throws Exception {
    Files.write(output, zipBytes(entries));
  }
}
