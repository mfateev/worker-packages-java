package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;
import static org.junit.Assert.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

public class PrivateJacksonIntegrationTest {
  @Rule public Timeout timeout = Timeout.seconds(60);
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  Path temp;

  @Before
  public void initializeTemporaryFolder() {
    temp = temporaryFolder.getRoot().toPath();
  }

  @Test
  public void privateCoreAndDatabindCoexistWithSharedAnnotationsAndHostConverter()
      throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(
        FixtureCompiler.buildPrivateJackson(temp.resolve("fixtures"), 2),
        packages.resolve("orders.twp"));
    String queue = "private-jackson-" + UUID.randomUUID();
    try (io.temporal.testing.TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      io.temporal.client.WorkflowClient client = client(env.getWorkflowServiceStubs(), "orders");
      try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host host =
          host(client, packages, temp.resolve("cache"), queue)) {
        io.temporal.worker.packages.api.PackageRef ref =
            host.plugin.runtime().catalog().require("orders", 2).ref();
        ClassLoader loader = host.plugin.runtime().classLoader(ref);
        assertNotSame(ObjectMapper.class, loader.loadClass(ObjectMapper.class.getName()));
        assertNotSame(
            com.fasterxml.jackson.core.JsonFactory.class,
            loader.loadClass(com.fasterxml.jackson.core.JsonFactory.class.getName()));
        assertSame(JsonProperty.class, loader.loadClass(JsonProperty.class.getName()));
        host.start();
        io.temporal.client.WorkflowStub run =
            workflow(client, queue, "jackson-" + UUID.randomUUID(), "JacksonWorkflow");
        run.start(input("Ada"));
        Map<?, ?> result = run.getResult(20, TimeUnit.SECONDS, Map.class);
        assertEquals("2.14.3", result.get("databind"));
        assertEquals("2.14.3", result.get("core"));
        assertEquals(
            TestSupport.map("customer_name", "Ada"),
            new ObjectMapper().readValue((String) result.get("wire"), Map.class));
        assertEquals(
            "private dependencies do not replace the host mapper",
            "2.15.4",
            new ObjectMapper().version().toString());
        io.temporal.client.WorkflowStub ordinary =
            workflow(client, queue, "host-json-" + UUID.randomUUID(), "PayloadWorkflow");
        ordinary.start(java.util.Arrays.asList(input("Still shared")), false);
        assertEquals(
            java.util.Arrays.asList(TestSupport.map("customer_name", "Still shared")),
            ordinary.getResult(20, TimeUnit.SECONDS, java.util.List.class));
      }
    }
  }
}
