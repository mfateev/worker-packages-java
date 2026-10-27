package io.temporal.worker.packages.acceptance;

import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.concurrent.TimeUnit;

/** Separate JVM entrypoint: no in-memory loader, catalog, or workflow state is shared. */
public final class SubprocessWorker {
  public static void main(String[] args) throws Exception {
    if (args.length != 4)
      throw new IllegalArgumentException("target packages cache taskQueue required");
    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(TestServices.options().setTarget(args[0]).build());
    try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host host =
        IntegrationSupport.host(
            IntegrationSupport.client(service),
            java.nio.file.Paths.get(args[1]),
            java.nio.file.Paths.get(args[2]),
            args[3])) {
      host.start();
      System.out.println("WORKER_READY");
      System.out.flush();
      System.in.read();
    } finally {
      service.shutdown();
      service.awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}
