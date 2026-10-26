package io.temporal.worker.packages.tooling;

import io.temporal.worker.packages.api.PackageRef;
import java.nio.file.Path;
import java.util.Objects;

public final class ValidatedPackage {
  private final Path archive;
  private final PackageManifest manifest;
  private final PackageRef ref;
  private final ValidationLimits limits;

  public ValidatedPackage(
      Path archive, PackageManifest manifest, PackageRef ref, ValidationLimits limits) {
    archive = archive.toAbsolutePath().normalize();
    this.archive = archive;
    this.manifest = manifest;
    this.ref = ref;
    this.limits = limits;
  }

  public Path archive() {
    return archive;
  }

  public PackageManifest manifest() {
    return manifest;
  }

  public PackageRef ref() {
    return ref;
  }

  public ValidationLimits limits() {
    return limits;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof ValidatedPackage)) return false;
    ValidatedPackage that = (ValidatedPackage) other;
    return Objects.equals(archive, that.archive)
        && Objects.equals(manifest, that.manifest)
        && Objects.equals(ref, that.ref)
        && Objects.equals(limits, that.limits);
  }

  @Override
  public int hashCode() {
    return Objects.hash(archive, manifest, ref, limits);
  }

  @Override
  public String toString() {
    return "ValidatedPackage[archive="
        + archive
        + ", manifest="
        + manifest
        + ", ref="
        + ref
        + ", limits="
        + limits
        + "]";
  }

  public ValidatedPackage(Path archive, PackageManifest manifest, PackageRef ref) {
    this(archive, manifest, ref, ValidationLimits.DEFAULT);
  }

  public String sha256() {
    return ref.sha256();
  }
}
