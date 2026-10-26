package io.temporal.worker.packages.api;

/** Immutable content identity, suitable for durable dispatch headers. */
public final class PackageRef {
  private final String packageId;
  private final String packageVersion;
  private final String sha256;

  public PackageRef(String packageId, int packageVersion, String sha256) {
    this(packageId, validateVersion(packageVersion), sha256);
  }

  public PackageRef(String packageId, String packageVersion, String sha256) {
    if (packageId == null || !packageId.matches("[A-Za-z][A-Za-z0-9_.-]{0,99}")) {
      throw new IllegalArgumentException("Invalid package ID: " + packageId);
    }
    validateVersion(packageVersion);
    if (sha256 == null || !sha256.matches("[a-f0-9]{64}")) {
      throw new IllegalArgumentException("Expected full lowercase SHA-256 digest");
    }
    this.packageId = packageId;
    this.packageVersion = packageVersion;
    this.sha256 = sha256;
  }

  public String packageId() {
    return packageId;
  }

  public String packageVersion() {
    return packageVersion;
  }

  public String sha256() {
    return sha256;
  }

  /** Validates an opaque package version, such as a Git SHA or semantic version. */
  public static String validateVersion(String version) {
    if (version == null || !version.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")) {
      throw new IllegalArgumentException(
          "Invalid package version: "
              + version
              + "; use 1 to 128 letters, digits, dots, underscores or hyphens, starting with a letter or digit");
    }
    return version;
  }

  /** Converts an existing positive integer version into its string representation. */
  public static String validateVersion(int version) {
    if (version < 1) throw new IllegalArgumentException("Package version must be positive");
    return Integer.toString(version);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof PackageRef)) return false;
    PackageRef that = (PackageRef) other;
    return packageVersion.equals(that.packageVersion)
        && packageId.equals(that.packageId)
        && sha256.equals(that.sha256);
  }

  @Override
  public int hashCode() {
    int result = packageId.hashCode();
    result = 31 * result + packageVersion.hashCode();
    return 31 * result + sha256.hashCode();
  }

  @Override
  public String toString() {
    return "PackageRef[packageId="
        + packageId
        + ", packageVersion="
        + packageVersion
        + ", sha256="
        + sha256
        + "]";
  }
}
