package io.temporal.worker.packages.gradle;

import io.temporal.worker.packages.api.PackageRef;
import io.temporal.worker.packages.tooling.ManifestCodec;
import java.util.Collections;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;

/** Adds a workerPackage task to the normal Java assemble lifecycle. */
public final class WorkerPackageGradlePlugin implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    project.getPluginManager().apply("java-library");
    WorkerPackageExtension extension =
        project.getExtensions().create("workerPackage", WorkerPackageExtension.class);
    extension.getPackageId().convention(project.getName());
    extension.getPackageVersion().convention(project.provider(() -> packageVersion(project)));
    extension.getWorkflows().convention(Collections.emptyMap());
    extension.getActivities().convention(Collections.emptyMap());
    TaskProvider<Jar> jar = project.getTasks().named("jar", Jar.class);
    jar.configure(
        task -> {
          task.setPreserveFileTimestamps(false);
          task.setReproducibleFileOrder(true);
        });
    TaskProvider<WorkerPackageTask> packaging =
        project
            .getTasks()
            .register(
                "workerPackage",
                WorkerPackageTask.class,
                task -> {
                  task.setGroup("build");
                  task.setDescription(
                      "Builds a validated .twp archive with private runtime dependencies.");
                  task.getPackageId().set(extension.getPackageId());
                  task.getPackageVersion().set(extension.getPackageVersion());
                  task.getPatchesVersion().set(extension.getPatchesVersion());
                  task.getWorkflows().set(extension.getWorkflows());
                  task.getActivities().set(extension.getActivities());
                  task.getApplicationJar().set(jar.flatMap(Jar::getArchiveFile));
                  task.getRuntimeClasspath()
                      .from(project.getConfigurations().getByName("runtimeClasspath"));
                  task.getArchiveFile()
                      .convention(
                          project
                              .getLayout()
                              .getBuildDirectory()
                              .file(
                                  extension
                                      .getPackageId()
                                      .zip(
                                          extension.getPackageVersion(),
                                          (id, version) ->
                                              new String[] {
                                                ManifestCodec.logical(id),
                                                PackageRef.validateVersion(version)
                                              })
                                      .zip(
                                          extension
                                              .getPatchesVersion()
                                              .map(PackageRef::validateVersion)
                                              .orElse(""),
                                          (identity, target) ->
                                              "libs/"
                                                  + identity[0]
                                                  + "-"
                                                  + (target.isEmpty() ? "" : target + "-")
                                                  + identity[1]
                                                  + ".twp")));
                });
    project.getTasks().named("assemble").configure(task -> task.dependsOn(packaging));
  }

  private static String packageVersion(Project project) {
    try {
      return PackageRef.validateVersion(project.getVersion().toString());
    } catch (IllegalArgumentException invalid) {
      throw new GradleException(
          "Set workerPackage.packageVersion or project.version to a nonblank version token "
              + "such as a Git SHA or semantic version; "
              + invalid.getMessage(),
          invalid);
    }
  }
}
