package io.temporal.worker.packages.tooling;

import java.util.Objects;

public final class ValidationLimits {
  private final long archiveBytes;
  private final long expandedBytes;
  private final long manifestBytes;
  private final int entries;

  public ValidationLimits(long archiveBytes, long expandedBytes, long manifestBytes, int entries) {
    if (archiveBytes < 1 || expandedBytes < 1 || manifestBytes < 1 || entries < 1) {
      throw new IllegalArgumentException("All archive limits must be positive");
    }
    this.archiveBytes = archiveBytes;
    this.expandedBytes = expandedBytes;
    this.manifestBytes = manifestBytes;
    this.entries = entries;
  }

  public long archiveBytes() {
    return archiveBytes;
  }

  public long expandedBytes() {
    return expandedBytes;
  }

  public long manifestBytes() {
    return manifestBytes;
  }

  public int entries() {
    return entries;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof ValidationLimits)) return false;
    ValidationLimits that = (ValidationLimits) other;
    return archiveBytes == that.archiveBytes
        && expandedBytes == that.expandedBytes
        && manifestBytes == that.manifestBytes
        && entries == that.entries;
  }

  @Override
  public int hashCode() {
    return Objects.hash(archiveBytes, expandedBytes, manifestBytes, entries);
  }

  @Override
  public String toString() {
    return "ValidationLimits[archiveBytes="
        + archiveBytes
        + ", expandedBytes="
        + expandedBytes
        + ", manifestBytes="
        + manifestBytes
        + ", entries="
        + entries
        + "]";
  }

  public static final ValidationLimits DEFAULT =
      new ValidationLimits(256L << 20, 1L << 30, 1L << 20, 100_000);
}
