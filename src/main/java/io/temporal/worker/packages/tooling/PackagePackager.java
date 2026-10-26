package io.temporal.worker.packages.tooling;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.zip.*;

/** Reproducible wrapper packaging; input JAR bytes are preserved exactly. */
public final class PackagePackager {
  public ValidatedPackage pack(Path manifestFile, Path inputRoot, Path output) {
    ArchiveSupport.noSymlinkComponents(manifestFile);
    ArchiveSupport.regular(manifestFile);
    ManifestCodec codec = new ManifestCodec();
    PackageManifest manifest = codec.read(manifestFile);
    if (manifest.patchesVersion() != null
        && !output.getFileName().toString().equals(manifest.archiveFileName()))
      throw new PackageException("Patch archive must be named " + manifest.archiveFileName());
    Path temporary = null;
    try {
      Path destination = output.toAbsolutePath().normalize();
      Files.createDirectories(destination.getParent());
      temporary = Files.createTempFile(destination.getParent(), ".twp-pack-", ".tmp");
      ArchiveSupport.Budget budget = new ArchiveSupport.Budget(ValidationLimits.DEFAULT);
      try (ZipOutputStream zip =
          new ZipOutputStream(new BoundedOutput(Files.newOutputStream(temporary)))) {
        put(zip, "workflow-package.json", new ByteArrayInputStream(codec.write(manifest)), budget);
        for (String name : manifest.classpath()) {
          Path input = inputRoot.toAbsolutePath().normalize().resolve(name).normalize();
          if (!input.startsWith(inputRoot.toAbsolutePath().normalize()))
            throw new PackageException("JAR escapes input root: " + name);
          ArchiveSupport.noSymlinkComponents(inputRoot, input);
          ArchiveSupport.regular(input);
          try (InputStream in = Files.newInputStream(input)) {
            put(zip, name, in, budget);
          }
        }
      }
      ValidatedPackage checked = new PackageValidator().validate(temporary, false);
      Files.move(
          temporary,
          destination,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      return new ValidatedPackage(destination, checked.manifest(), checked.ref());
    } catch (IOException e) {
      throw new PackageException("Cannot package " + output + ": " + e.getMessage(), e);
    } finally {
      if (temporary != null)
        try {
          Files.deleteIfExists(temporary);
        } catch (IOException ignored) {
        }
    }
  }

  private static void put(
      ZipOutputStream zip, String name, InputStream in, ArchiveSupport.Budget budget)
      throws IOException {
    ZipEntry entry = new ZipEntry(name);
    // ZipEntry.setTime uses the local zone when encoding the DOS timestamp.
    // Avoid the epoch boundary, where Java 8 adds extra timestamp metadata.
    entry.setTime(
        LocalDateTime.of(1980, 1, 2, 0, 0)
            .atZone(java.time.ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli());
    zip.putNextEntry(entry);
    budget.transfer(in, zip, ValidationLimits.DEFAULT.expandedBytes());
    zip.closeEntry();
  }

  private static final class BoundedOutput extends FilterOutputStream {
    private long bytes;

    BoundedOutput(OutputStream out) {
      super(out);
    }

    private void add(long count) {
      bytes += count;
      if (bytes > ValidationLimits.DEFAULT.archiveBytes())
        throw new PackageException("Compressed archive size limit exceeded during packaging");
    }

    @Override
    public void write(int value) throws IOException {
      add(1);
      out.write(value);
    }

    @Override
    public void write(byte[] buffer, int offset, int count) throws IOException {
      add(count);
      out.write(buffer, offset, count);
    }
  }
}
