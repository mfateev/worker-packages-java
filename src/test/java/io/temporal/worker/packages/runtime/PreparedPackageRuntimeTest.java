package io.temporal.worker.packages.runtime;

import static org.junit.Assert.*;

import io.temporal.common.converter.EncodedValues;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.worker.packages.tooling.ManifestCodec;
import io.temporal.worker.packages.tooling.PackageManifest;
import io.temporal.worker.packages.tooling.PackagePackager;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.tools.ToolProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests real package metadata and first-use races without running workflow code outside the SDK.
 */
public class PreparedPackageRuntimeTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private Path temporary;

  @Before
  public void createTemporaryDirectory() {
    temporary = temporaryFolder.getRoot().toPath();
  }

  @Test
  public void concurrentFirstUseSharesOneLoaderAndOneActivityInstanceAcrossExportedMethods()
      throws Exception {
    try (Fixture fixture = fixture()) {
      PreparedPackageRuntime runtime = fixture.runtime;
      PackageRef ref = fixture.ref;
      ClassLoader expectedLoader = runtime.classLoader(ref);
      assertThrows(
          ClassNotFoundException.class,
          () -> getClass().getClassLoader().loadClass("fixture.RuntimeFixture"));
      assertNull(System.getProperty(fixture.initializationProperty));
      assertEquals(0, runtime.descriptor(ref).activities.size());

      int parallelism = 16;
      CountDownLatch ready = new CountDownLatch(parallelism);
      CountDownLatch start = new CountDownLatch(1);
      Set<ClassLoader> observed = java.util.concurrent.ConcurrentHashMap.newKeySet();
      java.util.concurrent.ExecutorService pool = Executors.newFixedThreadPool(parallelism);
      try {
        java.util.ArrayList<java.util.concurrent.Future<java.lang.Integer>> futures =
            new ArrayList<java.util.concurrent.Future<Integer>>();
        for (int i = 0; i < 64; i++) {
          String logicalType = i % 2 == 0 ? "Count" : "CountAgain";
          futures.add(
              pool.submit(
                  () -> {
                    ready.countDown();
                    assertTrue(start.await(10, TimeUnit.SECONDS));
                    ClassLoader before = Thread.currentThread().getContextClassLoader();
                    observed.add(runtime.classLoader(ref));
                    try {
                      return (Integer)
                          runtime.invokeActivity(ref, logicalType, new EncodedValues());
                    } finally {
                      assertSame(
                          "TCCL must be restored after every invocation",
                          before,
                          Thread.currentThread().getContextClassLoader());
                    }
                  }));
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        start.countDown();
        for (java.util.concurrent.Future<java.lang.Integer> future : futures)
          assertEquals(1, future.get(10, TimeUnit.SECONDS).intValue());
      } finally {
        start.countDown();
        pool.shutdownNow();
        assertTrue(
            "first-use executor must terminate", pool.awaitTermination(10, TimeUnit.SECONDS));
      }
      assertEquals(TestSupport.set(expectedLoader), observed);
      assertEquals("yes", System.getProperty(fixture.initializationProperty));
      Class<?> implementation = expectedLoader.loadClass("fixture.RuntimeFixture$CounterImpl");
      assertEquals(1, implementation.getMethod("created").invoke(null));
      assertEquals(1, runtime.descriptor(ref).activityInstances.size());
      assertEquals(
          "immutable metadata cached once per logical method",
          2,
          runtime.descriptor(ref).activities.size());
      assertSame(expectedLoader, implementation.getClassLoader());
    }
  }

  @Test
  public void lazyActivityMetadataDoesNotInitializeOrConstructImplementation() throws Exception {
    try (Fixture fixture = fixture()) {
      assertNull(System.getProperty(fixture.initializationProperty));
      assertNotNull((Object) fixture.runtime.descriptor(fixture.ref).activity("Count"));
      assertNull(System.getProperty(fixture.initializationProperty));
      assertEquals(0, fixture.runtime.descriptor(fixture.ref).activityInstances.size());
    }
  }

  @Test
  public void activityEntrypointsRejectUnsupportedClassesConstructorsAndNames() throws Exception {
    try (Fixture fixture = fixture()) {
      for (String type :
          java.util.Arrays.asList(
              "Hidden",
              "Abstract",
              "NoDefault",
              "PrivateDefault",
              "InterfaceOnly",
              "Duplicate",
              "ImplAnnotation",
              "Absent")) {
        RuntimeException failure =
            assertThrows(
                type,
                RuntimeException.class,
                () -> fixture.runtime.descriptor(fixture.ref).activity(type));
        assertNotNull(type, failure.getMessage());
      }
      assertEquals(
          "metadata errors must not construct application activities",
          0,
          fixture.runtime.descriptor(fixture.ref).activityInstances.size());
      assertNull(System.getProperty(fixture.initializationProperty));
    }
  }

  @Test
  public void workflowMetadataRequiresSingleNamedOrdinaryPublicNoArgumentEntrypoint()
      throws Exception {
    try (Fixture fixture = fixture()) {
      assertNotNull((Object) fixture.runtime.descriptor(fixture.ref).workflow("Example"));
      for (String type :
          java.util.Arrays.asList(
              "WrongName",
              "First",
              "Initialized",
              "Dynamic",
              "NoDefaultWorkflow",
              "AbstractWorkflow",
              "HiddenWorkflow")) {
        RuntimeException failure =
            assertThrows(
                type,
                RuntimeException.class,
                () -> fixture.runtime.descriptor(fixture.ref).workflow(type));
        assertNotNull(type, failure.getMessage());
      }
      assertNull(
          "workflow metadata must not initialize any application implementation",
          System.getProperty(fixture.initializationProperty));
      assertEquals(
          "only successful workflow metadata may be cached",
          1,
          fixture.runtime.descriptor(fixture.ref).workflows.size());
    }
  }

  @Test
  public void applicationFailureIsUnwrappedAndTcclIsRestored() throws Exception {
    try (Fixture fixture = fixture()) {
      ClassLoader before = Thread.currentThread().getContextClassLoader();
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () -> fixture.runtime.invokeActivity(fixture.ref, "Throw", new EncodedValues()));
      assertEquals("business failure", failure.getMessage());
      assertSame(before, Thread.currentThread().getContextClassLoader());
    }
  }

  private Fixture fixture() throws Exception {
    String property = "container.test.initialized." + UUID.randomUUID();
    Path input = Files.createDirectory(temporary.resolve("input"));
    Path source = input.resolve("src/fixture/RuntimeFixture.java");
    Files.createDirectories(source.getParent());
    TestSupport.writeString(source, fixtureSource(property));
    Path classes = Files.createDirectory(input.resolve("classes"));
    java.io.ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
    int exit =
        ToolProvider.getSystemJavaCompiler()
            .run(
                null,
                diagnostics,
                diagnostics,
                "-source",
                "8",
                "-target",
                "8",
                "-proc:none",
                "-parameters",
                "-classpath",
                compilerClasspath(),
                "-d",
                classes.toString(),
                source.toString());
    assertEquals(
        new String(diagnostics.toByteArray(), java.nio.charset.StandardCharsets.UTF_8), 0, exit);
    Path jar = input.resolve("app.jar");
    try (JarOutputStream zip = new JarOutputStream(Files.newOutputStream(jar));
        java.util.stream.Stream<java.nio.file.Path> paths = Files.walk(classes)) {
      for (Path file :
          paths
              .filter(Files::isRegularFile)
              .sorted()
              .collect(java.util.stream.Collectors.toList())) {
        zip.putNextEntry(new JarEntry(classes.relativize(file).toString().replace('\\', '/')));
        Files.copy(file, zip);
        zip.closeEntry();
      }
    }
    List<PackageManifest.Entrypoint> workflows =
        java.util.Arrays.asList(
            entry("Example", "ValidWorkflow"), entry("WrongName", "ValidWorkflow"),
            entry("First", "MultipleWorkflows"), entry("Initialized", "InitializedWorkflow"),
            entry("Dynamic", "DynamicImplementation"),
                entry("NoDefaultWorkflow", "NoDefaultWorkflow"),
            entry("AbstractWorkflow", "AbstractWorkflow"),
                entry("HiddenWorkflow", "HiddenWorkflow"));
    List<PackageManifest.Entrypoint> activities =
        java.util.Arrays.asList(
            entry("Count", "CounterImpl"),
            entry("CountAgain", "CounterImpl"),
            entry("Throw", "ThrowImpl"),
            entry("Hidden", "HiddenActivity"),
            entry("Abstract", "AbstractActivity"),
            entry("NoDefault", "NoDefaultActivity"),
            entry("PrivateDefault", "PrivateDefaultActivity"),
            entry("InterfaceOnly", "InterfaceOnly"),
            entry("Duplicate", "DuplicateActivity"),
            entry("ImplAnnotation", "ImplAnnotationActivity"),
            entry("Absent", "CounterImpl"));
    PackageManifest manifest =
        new PackageManifest(
            1,
            1,
            "fixture",
            "release-fixture",
            8,
            "temporal-default-v1",
            java.util.Arrays.asList("app.jar"),
            workflows,
            activities);
    Path manifestPath = input.resolve("workflow-package.json");
    Files.write(manifestPath, new ManifestCodec().write(manifest));
    Path packages = Files.createDirectory(temporary.resolve("packages"));
    io.temporal.worker.packages.tooling.ValidatedPackage validated =
        new PackagePackager().pack(manifestPath, input, packages.resolve("fixture.twp"));
    PreparedPackageRuntime runtime =
        PreparedPackageRuntime.open(packages, temporary.resolve("cache"));
    return new Fixture(runtime, validated.ref(), property);
  }

  private static PackageManifest.Entrypoint entry(String type, String implementation) {
    return new PackageManifest.Entrypoint(type, "fixture.RuntimeFixture$" + implementation);
  }

  private static String compilerClasspath() throws Exception {
    // Gradle may use a test URLClassLoader instead of placing dependencies on java.class.path.
    Set<String> paths =
        new LinkedHashSet<>(
            java.util.Arrays.asList(
                System.getProperty("java.class.path").split(java.io.File.pathSeparator)));
    for (ClassLoader loader = PreparedPackageRuntimeTest.class.getClassLoader();
        loader != null;
        loader = loader.getParent()) {
      if (loader instanceof URLClassLoader) {
        URLClassLoader urls = (URLClassLoader) loader;
        for (URL url : urls.getURLs())
          if (url.getProtocol().equals("file"))
            paths.add(java.nio.file.Paths.get(url.toURI()).toString());
      }
    }
    for (Class<?> type :
        java.util.Arrays.asList(
            io.temporal.workflow.Workflow.class,
            EncodedValues.class,
            io.temporal.worker.packages.api.PackageRef.class)) {
      java.security.CodeSource origin = type.getProtectionDomain().getCodeSource();
      if (origin != null && origin.getLocation().getProtocol().equals("file"))
        paths.add(java.nio.file.Paths.get(origin.getLocation().toURI()).toString());
    }
    return String.join(java.io.File.pathSeparator, paths);
  }

  private static final class Fixture implements AutoCloseable {
    private final PreparedPackageRuntime runtime;
    private final PackageRef ref;
    private final String initializationProperty;

    Fixture(PreparedPackageRuntime runtime, PackageRef ref, String initializationProperty) {
      this.runtime = runtime;
      this.ref = ref;
      this.initializationProperty = initializationProperty;
    }

    @Override
    public void close() throws Exception {
      try {
        runtime.close();
      } finally {
        System.clearProperty(initializationProperty);
      }
    }
  }

  private static String fixtureSource(String initializationProperty) {
    return ("package fixture;\n"
            + "import io.temporal.activity.ActivityInterface;\n"
            + "import io.temporal.activity.ActivityMethod;\n"
            + "import io.temporal.common.converter.EncodedValues;\n"
            + "import io.temporal.workflow.DynamicWorkflow;\n"
            + "import io.temporal.workflow.WorkflowInterface;\n"
            + "import io.temporal.workflow.WorkflowMethod;\n"
            + "import io.temporal.workflow.WorkflowInit;\n"
            + "import java.util.concurrent.atomic.AtomicInteger;\n"
            + "public final class RuntimeFixture {\n"
            + "  private static final String INITIALIZATION_PROPERTY = \"%s\";\n"
            + "  @ActivityInterface public interface Counter {\n"
            + "    @ActivityMethod(name=\"Count\") int count();\n"
            + "    @ActivityMethod(name=\"CountAgain\") int countAgain();\n"
            + "  }\n"
            + "  public static final class CounterImpl implements Counter {\n"
            + "    static { System.setProperty(INITIALIZATION_PROPERTY, \"yes\"); }\n"
            + "    private static final AtomicInteger CREATED = new AtomicInteger();\n"
            + "    private final int instance = CREATED.incrementAndGet();\n"
            + "    public CounterImpl() { checkLoader(); }\n"
            + "    public int count() { checkLoader(); return instance; }\n"
            + "    public int countAgain() { checkLoader(); return instance; }\n"
            + "    public static int created() { return CREATED.get(); }\n"
            + "    private void checkLoader() {\n"
            + "      if (Thread.currentThread().getContextClassLoader() != getClass().getClassLoader()) {\n"
            + "        throw new IllegalStateException(\"wrong package TCCL\");\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "  @ActivityInterface public interface ThrowApi { @ActivityMethod(name=\"Throw\") int call(); }\n"
            + "  public static final class ThrowImpl implements ThrowApi {\n"
            + "    public ThrowImpl() {}\n"
            + "    public int call() { throw new IllegalArgumentException(\"business failure\"); }\n"
            + "  }\n"
            + "  @ActivityInterface public interface HiddenApi { @ActivityMethod(name=\"Hidden\") int call(); }\n"
            + "  static final class HiddenActivity implements HiddenApi { public HiddenActivity() {} public int call() { return 1; } }\n"
            + "  @ActivityInterface public interface AbstractApi { @ActivityMethod(name=\"Abstract\") int call(); }\n"
            + "  public abstract static class AbstractActivity implements AbstractApi { public AbstractActivity() {} }\n"
            + "  @ActivityInterface public interface NoDefaultApi { @ActivityMethod(name=\"NoDefault\") int call(); }\n"
            + "  public static final class NoDefaultActivity implements NoDefaultApi {\n"
            + "    public NoDefaultActivity(int value) {} public int call() { return 1; }\n"
            + "  }\n"
            + "  @ActivityInterface public interface PrivateDefaultApi { @ActivityMethod(name=\"PrivateDefault\") int call(); }\n"
            + "  public static final class PrivateDefaultActivity implements PrivateDefaultApi {\n"
            + "    private PrivateDefaultActivity() {} public int call() { return 1; }\n"
            + "  }\n"
            + "  @ActivityInterface public interface InterfaceOnly { @ActivityMethod(name=\"InterfaceOnly\") int call(); }\n"
            + "  @ActivityInterface public interface DuplicateApi {\n"
            + "    @ActivityMethod(name=\"Duplicate\") int first(); @ActivityMethod(name=\"Duplicate\") int second();\n"
            + "  }\n"
            + "  public static final class DuplicateActivity implements DuplicateApi {\n"
            + "    public DuplicateActivity() {} public int first() { return 1; } public int second() { return 2; }\n"
            + "  }\n"
            + "  @ActivityInterface public interface ImplAnnotationApi { @ActivityMethod(name=\"ImplAnnotation\") int call(); }\n"
            + "  public static final class ImplAnnotationActivity implements ImplAnnotationApi {\n"
            + "    public ImplAnnotationActivity() {} @ActivityMethod(name=\"ImplAnnotation\") public int call() { return 1; }\n"
            + "  }\n"
            + "  @WorkflowInterface public interface ExampleApi { @WorkflowMethod(name=\"Example\") String run(); }\n"
            + "  public static final class ValidWorkflow implements ExampleApi {\n"
            + "    static { System.setProperty(INITIALIZATION_PROPERTY, \"workflow initialized\"); }\n"
            + "    public ValidWorkflow() {} public String run() { return \"valid\"; }\n"
            + "  }\n"
            + "  @WorkflowInterface public interface FirstApi { @WorkflowMethod(name=\"First\") String first(); }\n"
            + "  @WorkflowInterface public interface SecondApi { @WorkflowMethod(name=\"Second\") String second(); }\n"
            + "  public static final class MultipleWorkflows implements FirstApi, SecondApi {\n"
            + "    static { System.setProperty(INITIALIZATION_PROPERTY, \"workflow initialized\"); }\n"
            + "    public MultipleWorkflows() {} public String first() { return \"one\"; } public String second() { return \"two\"; }\n"
            + "  }\n"
            + "  @WorkflowInterface public interface InitializedApi { @WorkflowMethod(name=\"Initialized\") String run(String input); }\n"
            + "  public static final class InitializedWorkflow implements InitializedApi {\n"
            + "    static { System.setProperty(INITIALIZATION_PROPERTY, \"workflow initialized\"); }\n"
            + "    @WorkflowInit public InitializedWorkflow(String input) {} public String run(String input) { return input; }\n"
            + "  }\n"
            + "  public static final class DynamicImplementation implements DynamicWorkflow {\n"
            + "    static { System.setProperty(INITIALIZATION_PROPERTY, \"workflow initialized\"); }\n"
            + "    public DynamicImplementation() {} public Object execute(EncodedValues values) { return \"dynamic\"; }\n"
            + "  }\n"
            + "  @WorkflowInterface public interface NoDefaultWorkflowApi { @WorkflowMethod(name=\"NoDefaultWorkflow\") String run(); }\n"
            + "  public static final class NoDefaultWorkflow implements NoDefaultWorkflowApi {\n"
            + "    static { System.setProperty(INITIALIZATION_PROPERTY, \"workflow initialized\"); }\n"
            + "    public NoDefaultWorkflow(String input) {} public String run() { return \"bad\"; }\n"
            + "  }\n"
            + "  @WorkflowInterface public interface AbstractWorkflowApi { @WorkflowMethod(name=\"AbstractWorkflow\") String run(); }\n"
            + "  public abstract static class AbstractWorkflow implements AbstractWorkflowApi {\n"
            + "    static { System.setProperty(INITIALIZATION_PROPERTY, \"workflow initialized\"); }\n"
            + "    public AbstractWorkflow() {}\n"
            + "  }\n"
            + "  @WorkflowInterface public interface HiddenWorkflowApi { @WorkflowMethod(name=\"HiddenWorkflow\") String run(); }\n"
            + "  static final class HiddenWorkflow implements HiddenWorkflowApi {\n"
            + "    static { System.setProperty(INITIALIZATION_PROPERTY, \"workflow initialized\"); }\n"
            + "    public HiddenWorkflow() {} public String run() { return \"hidden\"; }\n"
            + "  }\n"
            + "}\n"
            + "")
        .replace("%s", initializationProperty);
  }
}
