package io.temporal.worker.packages.tooling;

import io.temporal.worker.packages.api.PackageRef;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.*;

/** Inspects archive bytes only; never loads or initializes application classes. */
public final class PackageValidator {
  private final ValidationLimits limits;

  public PackageValidator() {
    this(ValidationLimits.DEFAULT);
  }

  public PackageValidator(ValidationLimits limits) {
    this.limits = Objects.requireNonNull(limits);
  }

  public ValidatedPackage validate(Path archive) {
    return validate(archive, true);
  }

  ValidatedPackage validate(Path archive, boolean enforcePatchFileName) {
    Path temporary = null;
    try {
      ArchiveSupport.regular(archive);
      if (Files.size(archive) > limits.archiveBytes())
        throw new PackageException("Compressed archive size limit exceeded: " + archive);
      temporary = Files.createTempDirectory("twp-inspect-");
      Path snapshot = temporary.resolve("archive.twp");
      // Inspect and hash one immutable byte snapshot. Reopening the mutable source
      // path after inspection could otherwise pair one manifest with another digest.
      try (InputStream in = Files.newInputStream(archive);
          OutputStream out = Files.newOutputStream(snapshot)) {
        byte[] buffer = new byte[64 * 1024];
        long bytes = 0;
        int count;
        while ((count = in.read(buffer)) != -1) {
          bytes += count;
          if (bytes > limits.archiveBytes())
            throw new PackageException("Compressed archive size limit exceeded: " + archive);
          out.write(buffer, 0, count);
        }
      }
      ArchiveSupport.rejectSymlinks(snapshot, limits.entries());
      ArchiveSupport.Budget budget = new ArchiveSupport.Budget(limits);
      Map<String, ZipEntry> files = new LinkedHashMap<>();
      Set<String> names = new HashSet<>();
      try (ZipFile zip = new ZipFile(snapshot.toFile())) {
        Enumeration<? extends ZipEntry> entries = zip.entries();
        while (entries.hasMoreElements()) {
          ZipEntry entry = entries.nextElement();
          budget.entry();
          ArchiveSupport.safeName(entry.getName(), entry.isDirectory());
          if (!names.add(entry.getName()))
            throw new PackageException("Duplicate ZIP entry: " + entry.getName());
          if (!entry.isDirectory()) files.put(entry.getName(), entry);
        }
        ZipEntry manifestEntry = files.get("workflow-package.json");
        if (manifestEntry == null) throw new PackageException("Missing root workflow-package.json");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (InputStream in = zip.getInputStream(manifestEntry)) {
          budget.transfer(in, bytes, limits.manifestBytes());
        }
        PackageManifest manifest = new ManifestCodec().read(bytes.toByteArray());
        if (enforcePatchFileName
            && manifest.patchesVersion() != null
            && !archive.getFileName().toString().equals(manifest.archiveFileName()))
          throw new PackageException("Patch archive must be named " + manifest.archiveFileName());
        Set<String> allowed = new HashSet<>(manifest.classpath());
        allowed.add("workflow-package.json");
        if (!files.keySet().equals(allowed))
          throw new PackageException(
              "Archive files differ from manifest classpath: found "
                  + files.keySet()
                  + ", expected "
                  + allowed);
        Map<String, String> classes = new HashMap<>();
        for (int i = 0; i < manifest.classpath().size(); i++) {
          String jarName = manifest.classpath().get(i);
          Path jar = temporary.resolve(i + ".jar");
          try (InputStream in = zip.getInputStream(files.get(jarName));
              OutputStream out = Files.newOutputStream(jar)) {
            budget.transfer(in, out, limits.expandedBytes());
          }
          inspectJar(jar, jarName, budget, classes);
        }
        String digest = ArchiveSupport.digest(snapshot, limits.archiveBytes());
        return new ValidatedPackage(
            archive,
            manifest,
            new PackageRef(manifest.packageId(), manifest.packageVersion(), digest),
            limits);
      }
    } catch (IOException | IllegalArgumentException e) {
      throw new PackageException("Invalid package " + archive + ": " + e.getMessage(), e);
    } catch (PackageException e) {
      throw new PackageException("Invalid package " + archive + ": " + e.getMessage(), e);
    } finally {
      if (temporary != null)
        try {
          ArchiveSupport.deleteTree(temporary);
        } catch (IOException ignored) {
          /* Original failure takes priority. */
        }
    }
  }

  private void inspectJar(
      Path jar, String jarName, ArchiveSupport.Budget budget, Map<String, String> classes)
      throws IOException {
    ArchiveSupport.rejectSymlinks(jar, limits.entries());
    Set<String> names = new HashSet<>();
    try (ZipFile zip = new ZipFile(jar.toFile())) {
      Enumeration<? extends ZipEntry> entries = zip.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        budget.entry();
        String name = entry.getName();
        ArchiveSupport.safeName(name, entry.isDirectory());
        if (!names.add(name))
          throw new PackageException("Duplicate JAR entry in " + jarName + ": " + name);
        if (entry.isDirectory()) continue;
        if (name.equalsIgnoreCase("META-INF/INDEX.LIST"))
          throw new PackageException("Forbidden META-INF/INDEX.LIST in " + jarName);
        try (InputStream in = zip.getInputStream(entry)) {
          if (name.equalsIgnoreCase("META-INF/MANIFEST.MF")) {
            ByteArrayOutputStream manifestBytes = new ByteArrayOutputStream();
            budget.transfer(in, manifestBytes, limits.manifestBytes());
            Manifest metadata = new Manifest(new ByteArrayInputStream(manifestBytes.toByteArray()));
            if (metadata.getMainAttributes().containsKey(Attributes.Name.CLASS_PATH)
                || metadata.getEntries().values().stream()
                    .anyMatch(a -> a.containsKey(Attributes.Name.CLASS_PATH))) {
              throw new PackageException("Forbidden JAR manifest Class-Path in " + jarName);
            }
          } else budget.transfer(in, ArchiveSupport.DISCARD, limits.expandedBytes());
        }
        if (!name.endsWith(".class")) continue;
        String logical = name;
        if (logical.startsWith("META-INF/versions/")) {
          String tail = logical.substring("META-INF/versions/".length());
          int slash = tail.indexOf('/');
          if (slash < 1 || !tail.substring(0, slash).matches("[1-9][0-9]*"))
            throw new PackageException("Invalid multi-release class path: " + name);
          logical = tail.substring(slash + 1);
        }
        if (logical.equals("module-info.class")) continue;
        String binary = logical.substring(0, logical.length() - 6).replace('/', '.');
        if (ReservedNamespaces.isReserved(binary))
          throw new PackageException(
              "Forbidden shared class " + binary + " in " + jarName + " entry " + name);
        String previous = classes.putIfAbsent(binary, jarName);
        if (previous != null && !previous.equals(jarName))
          throw new PackageException(
              "Duplicate class " + binary + " in " + previous + " and " + jarName);
      }
    } catch (ZipException e) {
      throw new PackageException("Invalid JAR " + jarName, e);
    }
  }
}
