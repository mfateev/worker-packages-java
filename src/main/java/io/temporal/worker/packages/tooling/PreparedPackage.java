package io.temporal.worker.packages.tooling;

import io.temporal.worker.packages.api.PackageRef;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class PreparedPackage {
  private final ValidatedPackage source;
  private final Path directory;
  private final List<Path> classpath;

  public PreparedPackage(ValidatedPackage source, Path directory, List<Path> classpath) {
    classpath = new ArrayList<>(classpath);
    classpath.forEach(Objects::requireNonNull);
    classpath = Collections.unmodifiableList(classpath);
    this.source = source;
    this.directory = directory;
    this.classpath = classpath;
  }

  public ValidatedPackage source() {
    return source;
  }

  public Path directory() {
    return directory;
  }

  public List<Path> classpath() {
    return classpath;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof PreparedPackage)) return false;
    PreparedPackage that = (PreparedPackage) other;
    return Objects.equals(source, that.source)
        && Objects.equals(directory, that.directory)
        && Objects.equals(classpath, that.classpath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, directory, classpath);
  }

  @Override
  public String toString() {
    return "PreparedPackage[source="
        + source
        + ", directory="
        + directory
        + ", classpath="
        + classpath
        + "]";
  }

  public PackageRef ref() {
    return source.ref();
  }

  public PackageManifest manifest() {
    return source.manifest();
  }
}
