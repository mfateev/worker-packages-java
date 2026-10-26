package io.temporal.worker.packages.tooling;

import io.temporal.worker.packages.api.PackageRef;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class PackageManifest {
  @com.fasterxml.jackson.annotation.JsonProperty("formatVersion")
  private final int formatVersion;

  @com.fasterxml.jackson.annotation.JsonProperty("containerApiVersion")
  private final int containerApiVersion;

  @com.fasterxml.jackson.annotation.JsonProperty("packageId")
  private final String packageId;

  @com.fasterxml.jackson.annotation.JsonProperty("packageVersion")
  private final String packageVersion;

  @com.fasterxml.jackson.annotation.JsonProperty("javaRelease")
  private final int javaRelease;

  @com.fasterxml.jackson.annotation.JsonProperty("payloadEncoding")
  private final String payloadEncoding;

  @com.fasterxml.jackson.annotation.JsonProperty("classpath")
  private final List<String> classpath;

  @com.fasterxml.jackson.annotation.JsonProperty("workflows")
  private final List<Entrypoint> workflows;

  @com.fasterxml.jackson.annotation.JsonProperty("activities")
  private final List<Entrypoint> activities;

  @com.fasterxml.jackson.annotation.JsonProperty("patchesVersion")
  @com.fasterxml.jackson.annotation.JsonInclude(
      com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
  private final String patchesVersion;

  public PackageManifest(
      int formatVersion,
      int containerApiVersion,
      String packageId,
      int packageVersion,
      int javaRelease,
      String payloadEncoding,
      List<String> classpath,
      List<Entrypoint> workflows,
      List<Entrypoint> activities) {
    this(
        formatVersion,
        containerApiVersion,
        packageId,
        PackageRef.validateVersion(packageVersion),
        javaRelease,
        payloadEncoding,
        classpath,
        workflows,
        activities,
        null);
  }

  public PackageManifest(
      int formatVersion,
      int containerApiVersion,
      String packageId,
      String packageVersion,
      int javaRelease,
      String payloadEncoding,
      List<String> classpath,
      List<Entrypoint> workflows,
      List<Entrypoint> activities) {
    this(
        formatVersion,
        containerApiVersion,
        packageId,
        packageVersion,
        javaRelease,
        payloadEncoding,
        classpath,
        workflows,
        activities,
        null);
  }

  public PackageManifest(
      int formatVersion,
      int containerApiVersion,
      String packageId,
      String packageVersion,
      int javaRelease,
      String payloadEncoding,
      List<String> classpath,
      List<Entrypoint> workflows,
      List<Entrypoint> activities,
      String patchesVersion) {
    classpath = immutableCopy(classpath);
    workflows = immutableCopy(workflows);
    activities = immutableCopy(activities);
    this.formatVersion = formatVersion;
    this.containerApiVersion = containerApiVersion;
    this.packageId = packageId;
    this.packageVersion = PackageRef.validateVersion(packageVersion);
    this.patchesVersion =
        patchesVersion == null ? null : PackageRef.validateVersion(patchesVersion);
    if (this.packageVersion.equals(this.patchesVersion))
      throw new IllegalArgumentException(
          "A package cannot patch its own version: " + packageVersion);
    this.javaRelease = javaRelease;
    this.payloadEncoding = payloadEncoding;
    this.classpath = classpath;
    this.workflows = workflows;
    this.activities = activities;
  }

  public int formatVersion() {
    return formatVersion;
  }

  public int containerApiVersion() {
    return containerApiVersion;
  }

  public String packageId() {
    return packageId;
  }

  public String packageVersion() {
    return packageVersion;
  }

  public int javaRelease() {
    return javaRelease;
  }

  public String payloadEncoding() {
    return payloadEncoding;
  }

  public List<String> classpath() {
    return classpath;
  }

  public List<Entrypoint> workflows() {
    return workflows;
  }

  public List<Entrypoint> activities() {
    return activities;
  }

  /** Returns the logical version this package replaces, or null for a normal package. */
  public String patchesVersion() {
    return patchesVersion;
  }

  /** Returns the canonical archive filename, including the target version for a patch. */
  public String archiveFileName() {
    return packageId
        + "-"
        + (patchesVersion == null ? "" : patchesVersion + "-")
        + packageVersion
        + ".twp";
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof PackageManifest)) return false;
    PackageManifest that = (PackageManifest) other;
    return formatVersion == that.formatVersion
        && containerApiVersion == that.containerApiVersion
        && Objects.equals(packageId, that.packageId)
        && Objects.equals(packageVersion, that.packageVersion)
        && javaRelease == that.javaRelease
        && Objects.equals(payloadEncoding, that.payloadEncoding)
        && Objects.equals(classpath, that.classpath)
        && Objects.equals(workflows, that.workflows)
        && Objects.equals(activities, that.activities)
        && Objects.equals(patchesVersion, that.patchesVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        formatVersion,
        containerApiVersion,
        packageId,
        packageVersion,
        javaRelease,
        payloadEncoding,
        classpath,
        workflows,
        activities,
        patchesVersion);
  }

  @Override
  public String toString() {
    return "PackageManifest[formatVersion="
        + formatVersion
        + ", containerApiVersion="
        + containerApiVersion
        + ", packageId="
        + packageId
        + ", packageVersion="
        + packageVersion
        + ", javaRelease="
        + javaRelease
        + ", payloadEncoding="
        + payloadEncoding
        + ", classpath="
        + classpath
        + ", workflows="
        + workflows
        + ", activities="
        + activities
        + ", patchesVersion="
        + patchesVersion
        + "]";
  }

  private static <T> List<T> immutableCopy(List<T> values) {
    List<T> copy = new ArrayList<>(values);
    copy.forEach(Objects::requireNonNull);
    return Collections.unmodifiableList(copy);
  }

  public static final class Entrypoint {
    @com.fasterxml.jackson.annotation.JsonProperty("type")
    private final String type;

    @com.fasterxml.jackson.annotation.JsonProperty("implementation")
    private final String implementation;

    public Entrypoint(String type, String implementation) {
      this.type = type;
      this.implementation = implementation;
    }

    public String type() {
      return type;
    }

    public String implementation() {
      return implementation;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (!(other instanceof Entrypoint)) return false;
      Entrypoint that = (Entrypoint) other;
      return Objects.equals(type, that.type) && Objects.equals(implementation, that.implementation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, implementation);
    }

    @Override
    public String toString() {
      return "Entrypoint[type=" + type + ", implementation=" + implementation + "]";
    }
  }
}
