package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;

import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.testserver.TestServer;
import io.temporal.worker.packages.tooling.PackageCatalog;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Simple greeting example with distinct worker JVMs and one persistent local test service. */
public final class RestartDemo {
  public static void main(String[] args) throws Exception {
    Path build = Files.createDirectories(FixtureCompiler.root().resolve("build"));
    Path work =
        args.length == 0
            ? Files.createTempDirectory(build, "restart-demo-")
            : java.nio.file.Paths.get(args[0]);
    Files.createDirectories(work);
    Path packages = Files.createDirectories(work.resolve("packages"));
    Path original = ExamplePackages.copy(work.resolve("examples"), 1);
    Path newer = ExamplePackages.copy(work.resolve("examples"), 2);
    Files.copy(original, packages.resolve("greetings-1.twp"));
    int port = ProcessHost.availablePort();
    String target = "127.0.0.1:" + port;
    String queue = "demo-" + UUID.randomUUID();
    try (TestServer.PortBoundTestServer server = TestServer.createPortBoundServer(port, true)) {
      WorkflowServiceStubs service =
          WorkflowServiceStubs.newServiceStubs(TestServices.options().setTarget(target).build());
      try {
        io.temporal.client.WorkflowClient client = client(service, "greetings");
        WorkflowStub a = workflow(client, queue, "demo-A-" + UUID.randomUUID(), "GreetingWorkflow");
        // ProcessHost's worker only hosts packages; this controller client supplies start headers.
        try (io.temporal.worker.packages.acceptance.ProcessHost first =
            new ProcessHost(
                target, packages, work.resolve("cache-a"), queue, work.resolve("worker-a.log"))) {
          a.start();
          awaitGreetingWaiting(client, a, 1);
          System.out.println(
              "A is durably waiting; stopping worker JVM A while service remains alive");
        }
        Files.copy(newer, packages.resolve("greetings-2.twp"));
        PackageCatalog catalog = PackageCatalog.scan(packages);
        for (io.temporal.worker.packages.tooling.ValidatedPackage entry : catalog.entries()) {
          System.out.println(
              entry.ref() + " selectedForNewRuns=" + entry.ref().packageVersion().equals("2"));
        }
        try (io.temporal.worker.packages.acceptance.ProcessHost second =
            new ProcessHost(
                target, packages, work.resolve("cache-b"), queue, work.resolve("worker-b.log"))) {
          WorkflowStub b =
              workflow(client, queue, "demo-B-" + UUID.randomUUID(), "GreetingWorkflow");
          b.start();
          awaitGreetingWaiting(client, b, 2);
          a.signal("proceed");
          b.signal("proceed");
          String oldResult = result(client, a, 20, String.class);
          String newResult = result(client, b, 20, String.class);
          if (!oldResult.equals("Hello, World!")
              || !newResult.equals("Hi, multi-versioned worker!")) {
            throw new IllegalStateException(
                "Version isolation failed: " + oldResult + " / " + newResult);
          }
          System.out.println(
              "A: version=1 digest="
                  + catalog.require("greetings", 1).ref().sha256()
                  + " "
                  + oldResult);
          System.out.println(
              "B: version=2 digest="
                  + catalog.require("greetings", 2).ref().sha256()
                  + " "
                  + newResult);
          TestSupport.writeString(
              work.resolve("history-A.json"),
              client.fetchHistory(a.getExecution().getWorkflowId()).toJson(true));
          TestSupport.writeString(
              work.resolve("history-B.json"),
              client.fetchHistory(b.getExecution().getWorkflowId()).toJson(true));
        }
        System.out.println("Demo evidence: " + work.toAbsolutePath());
      } finally {
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }
}
