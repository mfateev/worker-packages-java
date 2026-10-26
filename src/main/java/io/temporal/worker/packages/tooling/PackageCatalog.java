package io.temporal.worker.packages.tooling;

import io.temporal.worker.packages.api.PackageRef;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Immutable validated startup snapshot; lookup methods never perform I/O. */
public final class PackageCatalog {
  private final NavigableMap<String, NavigableMap<String, ValidatedPackage>> packages;
  private final List<ValidatedPackage> entries;
  private final NavigableMap<String, ValidatedPackage> versions;
  private final Map<String, Map<String, ValidatedPackage>> patches;

  private PackageCatalog(List<ValidatedPackage> validated) {
    TreeMap<String, NavigableMap<String, ValidatedPackage>> mutable = new TreeMap<>();
    TreeMap<String, ValidatedPackage> allVersions = new TreeMap<>();
    for (ValidatedPackage p : validated) {
      ValidatedPackage duplicate = allVersions.putIfAbsent(p.ref().packageVersion(), p);
      if (duplicate != null)
        throw new PackageException(
            "Duplicate package version "
                + p.ref().packageVersion()
                + ": "
                + duplicate.ref().packageId()
                + " ("
                + duplicate.archive()
                + ") and "
                + p.ref().packageId()
                + " ("
                + p.archive()
                + ")");
      NavigableMap<String, ValidatedPackage> versions =
          mutable.computeIfAbsent(p.ref().packageId(), ignored -> new TreeMap<>());
      versions.put(p.ref().packageVersion(), p);
    }
    Map<String, Map<String, ValidatedPackage>> replacements = new TreeMap<>();
    for (ValidatedPackage patch : validated) {
      String target = patch.manifest().patchesVersion();
      if (target == null) continue;
      Map<String, ValidatedPackage> targets =
          replacements.computeIfAbsent(patch.ref().packageId(), ignored -> new TreeMap<>());
      ValidatedPackage previous = targets.putIfAbsent(target, patch);
      if (previous != null)
        throw new PackageException(
            "Multiple patches for "
                + patch.ref().packageId()
                + "/"
                + target
                + ": "
                + previous.archive()
                + " and "
                + patch.archive());
    }
    for (ValidatedPackage patch : validated) {
      String target = patch.manifest().patchesVersion();
      if (target == null) continue;
      ValidatedPackage physicalTarget = allVersions.get(target);
      if (physicalTarget != null
          && !physicalTarget.ref().packageId().equals(patch.ref().packageId()))
        throw new PackageException(
            "Patch "
                + patch.ref()
                + " targets a version installed for another package: "
                + physicalTarget.ref());
      effectivePatch(replacements.get(patch.ref().packageId()), target);
    }
    replacements.replaceAll((id, targets) -> Collections.unmodifiableMap(targets));
    patches = Collections.unmodifiableMap(replacements);
    mutable.replaceAll((key, value) -> Collections.unmodifiableNavigableMap(value));
    packages = Collections.unmodifiableNavigableMap(mutable);
    versions = Collections.unmodifiableNavigableMap(allVersions);
    entries = Collections.unmodifiableList(new ArrayList<>(allVersions.values()));
  }

  public static PackageCatalog scan(Path directory) {
    return scan(directory, ValidationLimits.DEFAULT);
  }

  public static PackageCatalog scan(Path directory, ValidationLimits limits) {
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) || !Files.isReadable(directory))
      throw new PackageException("Expected readable package directory: " + directory);
    PackageValidator validator = new PackageValidator(limits);
    try (Stream<Path> paths = Files.list(directory)) {
      return new PackageCatalog(
          paths
              .filter(p -> p.getFileName().toString().endsWith(".twp"))
              .filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS))
              .sorted()
              .map(validator::validate)
              .collect(Collectors.toList()));
    } catch (IOException e) {
      throw new PackageException("Cannot scan package directory: " + directory, e);
    }
  }

  public List<ValidatedPackage> entries() {
    return entries;
  }

  /** Returns deterministic, distinct physical versions and logical patch-target aliases. */
  public List<String> selectionVersions() {
    Set<String> available = new TreeSet<>(versions.keySet());
    for (Map<String, ValidatedPackage> targets : patches.values())
      available.addAll(targets.keySet());
    return Collections.unmodifiableList(new ArrayList<>(available));
  }

  public Set<String> packageIds() {
    return packages.navigableKeySet();
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }

  public List<String> versions(String packageId) {
    return Collections.unmodifiableList(new ArrayList<>(requireVersions(packageId).keySet()));
  }

  /** Returns the sole globally installed version; versions have no implicit ordering. */
  public String currentVersion(String packageId) {
    requireVersions(packageId);
    return currentVersion();
  }

  /** Returns the sole globally installed version, failing when a selection is ambiguous. */
  public String currentVersion() {
    if (versions.isEmpty()) throw new PackageException("No packages installed");
    if (versions.size() != 1)
      throw new PackageException(
          "Multiple package versions installed: "
              + versions.keySet()
              + "; specify a package version explicitly when starting a workflow");
    return versions.firstKey();
  }

  public ValidatedPackage require(String packageId, int version) {
    return require(packageId, PackageRef.validateVersion(version));
  }

  public ValidatedPackage require(String packageId, String version) {
    PackageRef.validateVersion(version);
    ValidatedPackage p = requireVersions(packageId).get(version);
    if (p == null)
      throw new PackageException("Missing package version " + packageId + "/" + version);
    return p;
  }

  public ValidatedPackage require(PackageRef ref) {
    ValidatedPackage p = require(ref.packageId(), ref.packageVersion());
    if (!p.ref().sha256().equals(ref.sha256()))
      throw new PackageException("Package digest mismatch: required " + ref + ", found " + p.ref());
    return p;
  }

  /**
   * Resolves an explicit patch before requiring the original archive or its pinned checksum.
   * Unpatched executions retain strict archive and digest matching.
   */
  public ValidatedPackage resolveForExecution(PackageRef pinned) {
    ValidatedPackage replacement = patch(pinned.packageId(), pinned.packageVersion());
    return replacement == null ? require(pinned) : replacement;
  }

  /** Returns a durable logical reference, including when only its patch is installed. */
  public PackageRef referenceForWorkflow(String packageId, String requestedVersion) {
    PackageRef.validateVersion(requestedVersion);
    ValidatedPackage original = requireVersions(packageId).get(requestedVersion);
    if (original != null) return original.ref();
    ValidatedPackage replacement = patch(packageId, requestedVersion);
    if (replacement != null)
      return new PackageRef(packageId, requestedVersion, replacement.ref().sha256());
    return require(packageId, requestedVersion).ref();
  }

  private ValidatedPackage patch(String packageId, String target) {
    return effectivePatch(patches.get(packageId), target);
  }

  private static ValidatedPackage effectivePatch(
      Map<String, ValidatedPackage> targets, String target) {
    if (targets == null) return null;
    Set<String> visited = new LinkedHashSet<>();
    ValidatedPackage effective = null;
    while (targets.containsKey(target)) {
      if (!visited.add(target))
        throw new PackageException("Patch cycle detected: " + visited + " -> " + target);
      effective = targets.get(target);
      target = effective.ref().packageVersion();
    }
    return effective;
  }

  private NavigableMap<String, ValidatedPackage> requireVersions(String packageId) {
    NavigableMap<String, ValidatedPackage> versions = packages.get(packageId);
    if (versions == null) throw new PackageException("Missing package ID: " + packageId);
    return versions;
  }
}
