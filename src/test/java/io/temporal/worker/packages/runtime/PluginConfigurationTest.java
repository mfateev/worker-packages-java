package io.temporal.worker.packages.runtime;

import static io.temporal.worker.packages.runtime.TestSupport.assertInstanceOf;
import static org.junit.Assert.*;

import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.common.interceptors.WorkerInterceptorBase;
import io.temporal.worker.VersionPreference;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.WorkerOptions;
import io.temporal.worker.packages.tooling.PackagePackager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.jar.JarOutputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PluginConfigurationTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private Path temporary;

  @Before
  public void createTemporaryDirectory() {
    temporary = temporaryFolder.getRoot().toPath();
  }

  @Test
  public void clientOnlyPluginAppendsInterceptorWithoutPackagesOrWorkerConfiguration()
      throws Exception {
    io.temporal.common.interceptors.WorkflowClientInterceptor original =
        new io.temporal.common.interceptors.WorkflowClientInterceptorBase() {};
    io.temporal.client.WorkflowClientOptions.Builder options =
        io.temporal.client.WorkflowClientOptions.newBuilder()
            .setNamespace("existing")
            .setInterceptors(original);
    try (WorkerPackagePlugin plugin = WorkerPackagePlugin.newBuilder().build()) {
      plugin.configureWorkflowClient(options);
      assertEquals("existing", options.build().getNamespace());
      assertSame(original, options.build().getInterceptors()[0]);
      assertInstanceOf(PackageClientInterceptor.class, options.build().getInterceptors()[1]);
      WorkerFactoryOptions.Builder factory = WorkerFactoryOptions.newBuilder();
      plugin.configureWorkerFactory(factory);
      assertNull(factory.build().getWorkerInterceptors());
      assertTrue(plugin.toString().contains("clientOnly=true"));
      assertThrows(PackageRuntimeException.class, plugin::runtime);
    }
  }

  @Test
  public void rejectsIncompleteWorkerConfigurationBeforePreparingPackages() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WorkerPackagePlugin.newBuilder().setCacheDirectory(temporary.resolve("cache")).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> WorkerPackagePlugin.newBuilder().setCurrentVersion("2").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> WorkerPackagePlugin.newBuilder().setVersionSelector(context -> "2").build());
    assertThrows(
        NullPointerException.class,
        () -> WorkerPackagePlugin.newBuilder().setPackagesDirectory(null));
    assertThrows(
        IllegalArgumentException.class,
        () -> WorkerPackagePlugin.newBuilder().setClientVersion("../bad"));
    assertFalse(Files.exists(temporary.resolve("cache")));
  }

  @Test
  public void builderCreatesIndependentPluginsAndLastWorkerPolicyWins() throws Exception {
    try (WorkerPackagePlugin ignored = open(builder -> {}, "2", "12")) {}
    WorkerPackagePlugin.Builder builder =
        WorkerPackagePlugin.newBuilder()
            .setPackagesDirectory(temporary.resolve("packages"))
            .setCacheDirectory(temporary.resolve("cache"))
            .setVersionSelector(
                context -> {
                  throw new AssertionError("Replaced selector must not run");
                })
            .setCurrentVersion("2");
    try (WorkerPackagePlugin first = builder.build();
        WorkerPackagePlugin second = builder.setCurrentVersion("12").build()) {
      assertTrue(first.toString().contains("currentVersion=2"));
      assertTrue(second.toString().contains("currentVersion=12"));
      assertNotSame(first.runtime(), second.runtime());
    }
  }

  @Test
  public void appendsInterceptorAndPreservesSettingsAndUserVersionProvider() throws Exception {
    try (WorkerPackagePlugin plugin = open()) {
      WorkerInterceptor original = new WorkerInterceptorBase();
      WorkerFactoryOptions.Builder factory =
          WorkerFactoryOptions.newBuilder()
              .setWorkerInterceptors(original)
              .setWorkflowCacheSize(27);
      plugin.configureWorkerFactory(factory);
      WorkerFactoryOptions result = factory.build();
      assertEquals(27, result.getWorkflowCacheSize());
      assertEquals(2, result.getWorkerInterceptors().length);
      assertSame(original, result.getWorkerInterceptors()[0]);
      assertInstanceOf(PackageWorkerInterceptor.class, result.getWorkerInterceptors()[1]);
      WorkerOptions.Builder managed = WorkerOptions.newBuilder();
      plugin.configureWorker("managed", managed);
      assertNull(managed.build().getPreferredVersionProvider());
      WorkerOptions.Builder unrelated = WorkerOptions.newBuilder();
      plugin.configureWorker("unrelated", unrelated);
      assertNull(unrelated.build().getPreferredVersionProvider());
      WorkerOptions.Builder custom =
          WorkerOptions.newBuilder().setPreferredVersionProvider(i -> VersionPreference.of(17));
      Object originalProvider = custom.build().getPreferredVersionProvider();
      plugin.configureWorker("managed", custom);
      assertSame(originalProvider, custom.build().getPreferredVersionProvider());
      assertThrows(
          PackageRuntimeException.class,
          () -> plugin.configureWorkerFactory(WorkerFactoryOptions.newBuilder()));
    }
  }

  @Test
  public void rejectsNullVersionSelector() {
    assertThrows(
        NullPointerException.class,
        () -> WorkerPackagePlugin.newBuilder().setVersionSelector(null));
  }

  @Test
  public void diagnosticsDescribeTheOnlyInstalledVersion() throws Exception {
    try (WorkerPackagePlugin plugin = open()) {
      String diagnostics = plugin.toString();
      assertTrue(diagnostics, diagnostics.contains("versionSelector=singleInstalled"));
      assertTrue(diagnostics, diagnostics.contains("currentVersion=release-2026.10"));
      assertTrue(diagnostics, diagnostics.contains("workflows=[Example]"));
      assertTrue(diagnostics, diagnostics.contains("activities=[]"));
    }
  }

  @Test
  public void diagnosticsDoNotInvokeConfiguredSelectorOrClaimACurrentVersion() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    Consumer<WorkerPackagePlugin.Builder> options =
        builder ->
            builder.setVersionSelector(
                context -> {
                  calls.incrementAndGet();
                  return context.getAvailableVersions().get(0);
                });
    try (WorkerPackagePlugin plugin = open(options, "2.0.0", "release-alpha")) {
      String diagnostics = plugin.toString();
      assertTrue(diagnostics, diagnostics.contains("versionSelector=configured"));
      assertTrue(diagnostics, diagnostics.contains("version=2.0.0"));
      assertTrue(diagnostics, diagnostics.contains("version=release-alpha"));
      assertFalse(diagnostics, diagnostics.contains("currentVersion="));
      assertEquals(0, calls.get());
    }
  }

  @Test
  public void multipleInstalledVersionsCanOpenBeforeASelectionPolicyIsNeeded() throws Exception {
    try (WorkerPackagePlugin plugin = open(builder -> {}, "2.0.0", "release-alpha")) {
      String diagnostics = plugin.toString();
      assertTrue(diagnostics, diagnostics.contains("versionSelector=singleInstalled"));
      assertFalse(diagnostics, diagnostics.contains("currentVersion="));
    }
  }

  @Test
  public void shutdownHooksForwardExactlyOnceAndKeepPreparedRuntimeOpen() throws Exception {
    try (WorkerPackagePlugin plugin = open()) {
      AtomicInteger workers = new AtomicInteger(), factories = new AtomicInteger();
      plugin.shutdownWorker("managed", null, (queue, worker) -> workers.incrementAndGet());
      plugin.shutdownWorkerFactory(null, factory -> factories.incrementAndGet());
      assertEquals(1, workers.get());
      assertEquals(1, factories.get());
      assertFalse(plugin.runtime().catalog().isEmpty());
    }
  }

  @Test
  public void failedAttachmentBeforeStartupCanReleaseLoaders() throws Exception {
    WorkerPackagePlugin plugin = open();
    plugin.configureWorkerFactory(WorkerFactoryOptions.newBuilder());
    plugin.close();
    assertThrows(PackageRuntimeException.class, () -> plugin.runtime().catalog());
    assertThrows(
        PackageRuntimeException.class,
        () -> plugin.configureWorker("managed", WorkerOptions.newBuilder()));
  }

  @Test
  public void activeReplayPreventsCleanupAndCleanupWorksAfterReplayFailure() throws Exception {
    WorkerPackagePlugin plugin = open();
    plugin.configureWorkerFactory(WorkerFactoryOptions.newBuilder());
    Exception marker = new Exception("replay failure");
    Exception actual =
        assertThrows(
            Exception.class,
            () ->
                plugin.replayWorkflowExecution(
                    null,
                    null,
                    (worker, history) -> {
                      assertThrows(PackageRuntimeException.class, plugin::close);
                      throw marker;
                    }));
    assertSame(marker, actual);
    plugin.close();
  }

  private WorkerPackagePlugin open() throws Exception {
    return open(builder -> {}, "release-2026.10");
  }

  private WorkerPackagePlugin open(
      Consumer<WorkerPackagePlugin.Builder> options, String... versions) throws Exception {
    Path input = Files.createDirectories(temporary.resolve("input"));
    try (JarOutputStream jar =
        new JarOutputStream(Files.newOutputStream(input.resolve("app.jar")))) {}
    Path manifest = input.resolve("workflow-package.json");
    Path packages = Files.createDirectories(temporary.resolve("packages"));
    for (int i = 0; i < versions.length; i++) {
      TestSupport.writeString(
          manifest,
          "{\"formatVersion\":1,\"containerApiVersion\":1,\"packageId\":\"test\",\"packageVersion\":\""
              + versions[i]
              + "\",\n"
              + "\"javaRelease\":8,\"payloadEncoding\":\"temporal-default-v1\",\n"
              + "\"classpath\":[\"app.jar\"],\"workflows\":[{\"type\":\"Example\",\"implementation\":\"example.Workflow\"}],\"activities\":[]}\n");
      new PackagePackager().pack(manifest, input, packages.resolve("test-" + i + ".twp"));
    }
    WorkerPackagePlugin.Builder pluginBuilder =
        WorkerPackagePlugin.newBuilder()
            .setPackagesDirectory(packages)
            .setCacheDirectory(temporary.resolve("cache"));
    options.accept(pluginBuilder);
    return pluginBuilder.build();
  }
}
