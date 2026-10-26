package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;
import static org.junit.Assert.*;

import example.worker.PackageWorker;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.testserver.TestServer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

public class PackageWorkerExampleTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public Timeout timeout = Timeout.seconds(90);

  @Test
  public void standaloneWorkerLoadsItsDirectoryArgumentAndShutsDownCleanly() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> PackageWorker.main(new String[0]));
    Path temp = temporaryFolder.getRoot().toPath();
    Path packages = Files.createDirectory(temp.resolve("packages with spaces"));
    ExamplePackages.copy(packages, 1);
    ExamplePackages.copy(packages, 2);
    String queue = "example-worker-" + UUID.randomUUID();
    int port = ProcessHost.availablePort();
    String target = "127.0.0.1:" + port;
    Path config = temp.resolve("temporal.toml");
    TestSupport.writeString(
        config,
        "[profile.worker-example]\naddress = \""
            + target
            + "\"\nnamespace = \"overridden-by-environment\"\n");
    Path log = temp.resolve("worker.log");
    try (TestServer.PortBoundTestServer server = TestServer.createPortBoundServer(port, true)) {
      WorkflowServiceStubs service =
          WorkflowServiceStubs.newServiceStubs(TestServices.options().setTarget(target).build());
      Process process = null;
      try {
        ProcessBuilder builder =
            new ProcessBuilder(
                Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("acceptance.classpath"),
                PackageWorker.class.getName(),
                packages.toString());
        builder.environment().keySet().removeIf(name -> name.startsWith("TEMPORAL_"));
        builder.environment().put("TEMPORAL_CONFIG_FILE", config.toString());
        builder.environment().put("TEMPORAL_PROFILE", "worker-example");
        builder.environment().put("TEMPORAL_NAMESPACE", "default");
        builder.environment().put("TEMPORAL_TASK_QUEUE", queue);
        builder.environment().put("TEMPORAL_PACKAGE_VERSION", "2");
        builder.environment().put("TEMPORAL_PACKAGE_CACHE_DIR", temp.resolve("cache").toString());
        process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        await(
            "example worker readiness",
            Duration.ofSeconds(25),
            () -> {
              try {
                return TestSupport.readString(log).contains("Worker polling " + queue);
              } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
              }
            });
        WorkflowClient client = client(service, "greetings");
        WorkflowStub run =
            workflow(client, queue, "example-" + UUID.randomUUID(), "GreetingWorkflow");
        run.start();
        awaitGreetingWaiting(client, run, 2);
        run.signal("proceed");
        assertEquals("Hi, multi-versioned worker!", result(client, run, 20, String.class));
        String diagnostics = TestSupport.readString(log);
        assertTrue(diagnostics.contains("versionSelector=configured"));
        assertTrue(diagnostics.contains("version=1 package=greetings"));
        assertTrue(diagnostics.contains("version=2 package=greetings"));
        assertTrue(diagnostics.contains("workflows=[GreetingWorkflow]"));
        assertTrue(diagnostics.contains("activities=[Greet]"));
        assertTrue(diagnostics.contains("activities=[GetName, Greet]"));
        process.destroy();
        assertTrue("Worker shutdown timed out", process.waitFor(30, TimeUnit.SECONDS));
        assertTrue(
            "Worker exit code " + process.exitValue(),
            process.exitValue() == 0 || process.exitValue() == 143);
        assertFalse(
            TestSupport.readString(log),
            TestSupport.readString(log).contains("Exception in thread"));
      } finally {
        if (process != null && process.isAlive()) {
          process.destroyForcibly();
          process.waitFor(10, TimeUnit.SECONDS);
        }
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }
}
