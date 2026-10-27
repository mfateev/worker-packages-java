package io.temporal.worker.packages.tooling;

import io.temporal.worker.packages.api.PackageRef;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Immutable validated startup snapshot; lookup methods never perform I/O. */
public final class PackageCatalog {
  private final List<ValidatedPackage> entries;
  private final NavigableSet<String> versions = new TreeSet<>();
  private final NavigableSet<String> available = new TreeSet<>();
  private final Map<String, Map<String, ValidatedPackage>> workflows;
  private final Map<String, Map<String, ValidatedPackage>> activities;
  private final Map<String, Map<String, ValidatedPackage>> workflowPatches;
  private final Map<String, Map<String, ValidatedPackage>> activityPatches;

  private PackageCatalog(List<ValidatedPackage> validated) {
    entries = Collections.unmodifiableList(new ArrayList<>(validated));
    workflows = index(true, false);
    activities = index(false, false);
    workflowPatches = index(true, true);
    activityPatches = index(false, true);
    for (String version : available) {
      for (String type : types(version, true)) effective(type, version, true);
      for (String type : types(version, false)) effective(type, version, false);
    }
  }

  private Map<String, Map<String, ValidatedPackage>> index(boolean workflow, boolean patch) {
    Map<String, Map<String, ValidatedPackage>> result = new TreeMap<>();
    for (ValidatedPackage archive : entries) {
      String version = patch ? archive.manifest().patchesVersion() : archive.ref().packageVersion();
      if (version == null) continue;
      available.add(version);
      if (!patch) versions.add(version);
      Map<String, ValidatedPackage> declarations =
          result.computeIfAbsent(version, ignored -> new TreeMap<>());
      for (PackageManifest.Entrypoint entry :
          workflow ? archive.manifest().workflows() : archive.manifest().activities()) {
        ValidatedPackage previous = declarations.putIfAbsent(entry.type(), archive);
        if (previous != null)
          throw new PackageException(
              (patch ? "Multiple patches for " : "Duplicate ")
                  + (workflow ? "workflow" : "activity")
                  + " type "
                  + entry.type()
                  + " for version "
                  + version
                  + ": "
                  + previous.archive()
                  + " and "
                  + archive.archive());
      }
    }
    return result;
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
    return Collections.unmodifiableList(new ArrayList<>(available));
  }

  public Set<String> packageIds() {
    Set<String> ids = new TreeSet<>();
    for (ValidatedPackage archive : entries) ids.add(archive.ref().packageId());
    return Collections.unmodifiableSet(ids);
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }

  public List<String> versions(String packageId) {
    Set<String> found = new TreeSet<>();
    for (ValidatedPackage archive : entries)
      if (archive.ref().packageId().equals(packageId)) found.add(archive.ref().packageVersion());
    if (found.isEmpty()) throw new PackageException("Missing package ID: " + packageId);
    return Collections.unmodifiableList(new ArrayList<>(found));
  }

  public String currentVersion(String packageId) {
    versions(packageId);
    return currentVersion();
  }

  /** Returns the sole installed version; archive count does not affect selection. */
  public String currentVersion() {
    if (versions.isEmpty()) throw new PackageException("No packages installed");
    if (versions.size() != 1)
      throw new PackageException(
          "Multiple package versions installed: "
              + versions
              + "; specify a package version explicitly when starting a workflow");
    return versions.first();
  }

  public ValidatedPackage require(String packageId, int version) {
    return require(packageId, PackageRef.validateVersion(version));
  }

  public ValidatedPackage require(String packageId, String version) {
    PackageRef.validateVersion(version);
    ValidatedPackage found = null;
    for (ValidatedPackage archive : entries) {
      if (!archive.ref().packageId().equals(packageId)
          || !archive.ref().packageVersion().equals(version)) continue;
      if (found != null)
        throw new PackageException("Ambiguous archive identity " + packageId + "/" + version);
      found = archive;
    }
    if (found == null)
      throw new PackageException("Missing package version " + packageId + "/" + version);
    return found;
  }

  public ValidatedPackage require(PackageRef ref) {
    for (ValidatedPackage archive : entries) if (archive.ref().equals(ref)) return archive;
    ValidatedPackage found = require(ref.packageId(), ref.packageVersion());
    throw new PackageException(
        "Package digest mismatch: required " + ref + ", found " + found.ref());
  }

  /** Resolves an archive reference for callers that do not have a workflow/activity type. */
  public ValidatedPackage resolveForExecution(PackageRef pinned) {
    ValidatedPackage found = null;
    for (boolean workflow : new boolean[] {true, false}) {
      for (String type : types(pinned.packageVersion(), workflow)) {
        PackageRef reference = referenceForType(type, pinned.packageVersion(), workflow);
        if (!reference.packageId().equals(pinned.packageId())) continue;
        if (get(patchIndex(workflow), pinned.packageVersion(), type) == null) continue;
        ValidatedPackage replacement = effective(type, pinned.packageVersion(), workflow);
        if (found != null && found != replacement)
          throw new PackageException(
              "Archive has multiple type-specific patches; resolve by type: " + pinned);
        found = replacement;
      }
    }
    return found == null ? require(pinned) : found;
  }

  /** Resolves a type using explicit patches before checking the original checksum. */
  public ValidatedPackage resolveForExecution(PackageRef pinned, String type, boolean workflow) {
    ValidatedPackage selected = effective(type, pinned.packageVersion(), workflow);
    if (get(patchIndex(workflow), pinned.packageVersion(), type) == null
        && !selected.ref().equals(pinned))
      throw new PackageException(
          "Package digest mismatch: required " + pinned + ", found " + selected.ref());
    return selected;
  }

  /** Legacy archive lookup; normal workflow routing uses type and version. */
  public PackageRef referenceForWorkflow(String packageId, String requestedVersion) {
    for (ValidatedPackage archive : entries)
      if (archive.ref().packageId().equals(packageId)
          && archive.ref().packageVersion().equals(requestedVersion))
        return require(packageId, requestedVersion).ref();
    PackageRef found = null;
    for (String type : types(requestedVersion, true)) {
      PackageRef ref = referenceForWorkflowType(type, requestedVersion);
      if (!ref.packageId().equals(packageId)) continue;
      if (found != null && !found.equals(ref))
        throw new PackageException("Ambiguous archive identity " + packageId);
      found = ref;
    }
    if (found == null)
      throw new PackageException("Missing package version " + packageId + "/" + requestedVersion);
    return found;
  }

  public PackageRef referenceForWorkflowType(String type, String version) {
    return referenceForType(type, version, true);
  }

  public PackageRef referenceForActivityType(String type, String version) {
    return referenceForType(type, version, false);
  }

  private PackageRef referenceForType(String type, String version, boolean workflow) {
    PackageRef.validateVersion(version);
    ValidatedPackage selected = effective(type, version, workflow);
    ValidatedPackage original = get(physicalIndex(workflow), version, type);
    return original == null
        ? new PackageRef(selected.ref().packageId(), version, selected.ref().sha256())
        : original.ref();
  }

  private ValidatedPackage effective(String type, String version, boolean workflow) {
    Set<String> visited = new LinkedHashSet<>();
    String requested = version;
    while (true) {
      if (!visited.add(version))
        throw new PackageException(
            "Patch cycle detected for " + type + ": " + visited + " -> " + version);
      ValidatedPackage replacement = get(patchIndex(workflow), version, type);
      if (replacement == null) {
        ValidatedPackage selected = get(physicalIndex(workflow), version, type);
        if (selected == null)
          throw new PackageException(
              "Missing "
                  + (workflow ? "workflow" : "activity")
                  + " type "
                  + type
                  + " for version "
                  + requested);
        return selected;
      }
      version = replacement.ref().packageVersion();
    }
  }

  private Set<String> types(String version, boolean workflow) {
    Set<String> types = new TreeSet<>();
    Map<String, ValidatedPackage> physical = physicalIndex(workflow).get(version);
    Map<String, ValidatedPackage> patched = patchIndex(workflow).get(version);
    if (physical != null) types.addAll(physical.keySet());
    if (patched != null) types.addAll(patched.keySet());
    return types;
  }

  private Map<String, Map<String, ValidatedPackage>> physicalIndex(boolean workflow) {
    return workflow ? workflows : activities;
  }

  private Map<String, Map<String, ValidatedPackage>> patchIndex(boolean workflow) {
    return workflow ? workflowPatches : activityPatches;
  }

  private static ValidatedPackage get(
      Map<String, Map<String, ValidatedPackage>> index, String version, String type) {
    Map<String, ValidatedPackage> declarations = index.get(version);
    return declarations == null ? null : declarations.get(type);
  }
}
