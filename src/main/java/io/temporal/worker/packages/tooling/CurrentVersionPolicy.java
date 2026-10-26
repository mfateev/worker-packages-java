package io.temporal.worker.packages.tooling;

/** Selects a package version explicitly; version identifiers have no implicit ordering. */
@FunctionalInterface
public interface CurrentVersionPolicy {
  String currentVersion(String packageId);

  /** Selects the only globally installed version, failing if more than one is installed. */
  static CurrentVersionPolicy singleInstalled(PackageCatalog catalog) {
    return catalog::currentVersion;
  }

  /**
   * Compatibility alias for {@link #singleInstalled(PackageCatalog)}. No version ordering is
   * applied.
   *
   * @deprecated Use {@link #singleInstalled(PackageCatalog)} or supply an explicit policy.
   */
  @Deprecated
  static CurrentVersionPolicy highest(PackageCatalog catalog) {
    return singleInstalled(catalog);
  }
}
