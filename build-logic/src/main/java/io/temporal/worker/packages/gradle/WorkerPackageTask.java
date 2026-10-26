package io.temporal.worker.packages.gradle;

import io.temporal.worker.packages.tooling.ManifestCodec;
import io.temporal.worker.packages.tooling.PackageManifest;
import io.temporal.worker.packages.tooling.PackagePackager;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.inject.Inject;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

/**
 * Packages the application JAR and its private runtime JARs using the container's strict packager.
 */
@CacheableTask
public abstract class WorkerPackageTask extends DefaultTask {
  @Input
  public abstract Property<String> getPackageId();

  @Input
  public abstract Property<String> getPackageVersion();

  @Input
  @Optional
  public abstract Property<String> getPatchesVersion();

  @Input
  public abstract MapProperty<String, String> getWorkflows();

  @Input
  public abstract MapProperty<String, String> getActivities();

  @InputFile
  @PathSensitive(PathSensitivity.NAME_ONLY)
  public abstract RegularFileProperty getApplicationJar();

  @InputFiles
  @PathSensitive(PathSensitivity.NAME_ONLY)
  public abstract ConfigurableFileCollection getRuntimeClasspath();

  @OutputFile
  public abstract RegularFileProperty getArchiveFile();

  // Gradle decorates this abstract accessor to supply its file-system service.
  @SuppressWarnings("JavaxInjectOnAbstractMethod")
  @Inject
  protected abstract FileSystemOperations getFileSystemOperations();

  @TaskAction
  public void packageWorkflow() throws IOException {
    Path stage = getTemporaryDir().toPath().resolve("input");
    getFileSystemOperations().delete(spec -> spec.delete(stage));
    Files.createDirectories(stage);
    List<String> classpath = new ArrayList<>();
    copy(getApplicationJar().get().getAsFile(), "app", stage, classpath);
    List<File> dependencies = new ArrayList<>(getRuntimeClasspath().getFiles());
    dependencies.sort(Comparator.comparing(File::getName));
    Set<String> names = new HashSet<>();
    for (File dependency : dependencies) {
      if (!names.add(dependency.getName()))
        throw new GradleException("Runtime JAR filename collision: " + dependency.getName());
      copy(dependency, "lib", stage, classpath);
    }
    PackageManifest manifest =
        new PackageManifest(
            1,
            1,
            getPackageId().get(),
            getPackageVersion().get(),
            8,
            "temporal-default-v1",
            classpath,
            entrypoints(getWorkflows().get()),
            entrypoints(getActivities().get()),
            getPatchesVersion().getOrNull());
    Path manifestFile = stage.resolve("workflow-package.json");
    Files.write(manifestFile, new ManifestCodec().write(manifest));
    new PackagePackager().pack(manifestFile, stage, getArchiveFile().get().getAsFile().toPath());
  }

  private static List<PackageManifest.Entrypoint> entrypoints(Map<String, String> types) {
    List<PackageManifest.Entrypoint> result = new ArrayList<>();
    new TreeMap<>(types)
        .forEach(
            (type, implementation) ->
                result.add(new PackageManifest.Entrypoint(type, implementation)));
    return result;
  }

  private static void copy(File jar, String directory, Path stage, List<String> classpath)
      throws IOException {
    if (!jar.isFile() || !jar.getName().endsWith(".jar"))
      throw new GradleException("Worker packages require JAR runtime dependencies: " + jar);
    String name = directory + "/" + jar.getName();
    Path target = stage.resolve(name);
    Files.createDirectories(target.getParent());
    Files.copy(jar.toPath(), target, StandardCopyOption.REPLACE_EXISTING);
    classpath.add(name);
  }
}
