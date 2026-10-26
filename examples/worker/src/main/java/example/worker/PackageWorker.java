package example.worker;

import io.temporal.client.WorkflowClient;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.packages.runtime.WorkerPackagePlugin;
import java.nio.file.Paths;

/** A worker that loads worker packages from its single command-line argument. */
public class PackageWorker {
  public static void main(String[] args) throws Exception {
    if (args.length != 1)
      throw new IllegalArgumentException("Usage: example-worker <package-directory>");

    ClientConfigProfile profile = ClientConfigProfile.load();
    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(profile.toWorkflowServiceStubsOptions());
    WorkflowClient client = WorkflowClient.newInstance(service, profile.toWorkflowClientOptions());

    String queue = System.getenv().getOrDefault("TEMPORAL_TASK_QUEUE", "workflow-packages");
    String version = System.getenv("TEMPORAL_PACKAGE_VERSION");
    WorkerPackagePlugin.Options.Builder pluginOptions = WorkerPackagePlugin.Options.newBuilder();
    if (version != null) pluginOptions.setVersionSelector(context -> version);
    WorkerPackagePlugin plugin =
        WorkerPackagePlugin.open(
            Paths.get(args[0]),
            Paths.get(System.getenv().getOrDefault("TEMPORAL_PACKAGE_CACHE_DIR", ".package-cache")),
            queue,
            pluginOptions.build());

    WorkerFactory factory =
        WorkerFactory.newInstance(
            client, WorkerFactoryOptions.newBuilder().setPlugins(plugin).build());
    factory.newWorker(queue);
    factory.start();
    System.out.println("Worker polling " + queue + " with packages from " + args[0]);
    System.out.println(plugin);
  }
}
