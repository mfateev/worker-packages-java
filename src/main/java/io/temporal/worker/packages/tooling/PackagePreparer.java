package io.temporal.worker.packages.tooling;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Digest-addressed extraction with process and interprocess serialization and atomic publication.
 */
public final class PackagePreparer {
  private static final ConcurrentHashMap<Path, ReentrantLock> LOCAL_LOCKS =
      new ConcurrentHashMap<>();
  private final Path cache;

  public PackagePreparer(Path cache) {
    this.cache = cache.toAbsolutePath().normalize();
  }

  public PreparedPackage prepare(ValidatedPackage source) {
    Path temporary = null;
    Path key = cache.resolve(source.ref().sha256());
    ReentrantLock local = LOCAL_LOCKS.computeIfAbsent(key, ignored -> new ReentrantLock());
    local.lock();
    try {
      Files.createDirectories(cache);
      if (Files.isSymbolicLink(cache))
        throw new PackageException("Cache directory cannot be a symbolic link: " + cache);
      Path lockPath = cache.resolve(source.ref().sha256() + ".lock");
      if (Files.isSymbolicLink(lockPath))
        throw new PackageException("Cache lock cannot be a symbolic link: " + lockPath);
      try (FileChannel channel =
              FileChannel.open(
                  lockPath,
                  StandardOpenOption.CREATE,
                  StandardOpenOption.WRITE,
                  LinkOption.NOFOLLOW_LINKS);
          FileLock ignored = channel.lock()) {
        if (Files.exists(key, LinkOption.NOFOLLOW_LINKS)) return verify(source, key);
        temporary = Files.createTempDirectory(cache, ".preparing-");
        Path archive = temporary.resolve("archive.twp");
        ArchiveSupport.regular(source.archive());
        try (InputStream in = Files.newInputStream(source.archive());
            OutputStream out = Files.newOutputStream(archive)) {
          byte[] buffer = new byte[64 * 1024];
          long bytes = 0;
          int count;
          while ((count = in.read(buffer)) != -1) {
            bytes += count;
            if (bytes > source.limits().archiveBytes())
              throw new PackageException(
                  "Compressed archive size limit exceeded during preparation");
            out.write(buffer, 0, count);
          }
        }
        if (!ArchiveSupport.digest(archive, source.limits().archiveBytes())
            .equals(source.ref().sha256()))
          throw new PackageException("Archive changed before preparation: " + source.archive());
        ValidatedPackage copied = new PackageValidator(source.limits()).validate(archive, false);
        if (!copied.ref().equals(source.ref()) || !copied.manifest().equals(source.manifest()))
          throw new PackageException("Prepared archive identity mismatch: " + source.ref());
        Path jars = temporary.resolve("jars");
        Files.createDirectory(jars);
        try (ZipFile zip = new ZipFile(archive.toFile())) {
          for (String name : source.manifest().classpath()) {
            Path destination = jars.resolve(name).normalize();
            if (!destination.startsWith(jars))
              throw new PackageException("Unsafe extraction path: " + name);
            Files.createDirectories(destination.getParent());
            try (InputStream in = zip.getInputStream(zip.getEntry(name))) {
              Files.copy(in, destination);
            }
          }
        }
        Files.move(temporary, key, StandardCopyOption.ATOMIC_MOVE);
        temporary = null;
        return verify(source, key);
      }
    } catch (IOException e) {
      throw new PackageException("Cannot prepare " + source.ref() + ": " + e.getMessage(), e);
    } finally {
      if (temporary != null)
        try {
          ArchiveSupport.deleteTree(temporary);
        } catch (IOException ignored) {
        }
      local.unlock();
    }
  }

  private PreparedPackage verify(ValidatedPackage source, Path directory) throws IOException {
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS))
      throw new PackageException("Invalid prepared directory: " + directory);
    Path archive = directory.resolve("archive.twp");
    ArchiveSupport.regular(archive);
    if (!ArchiveSupport.digest(archive, source.limits().archiveBytes())
        .equals(source.ref().sha256()))
      throw new PackageException("Corrupt cached archive for " + source.ref());
    List<Path> classpath = new ArrayList<>();
    Set<Path> expected = new HashSet<>();
    expected.add(archive);
    Path jars = directory.resolve("jars");
    if (!Files.isDirectory(jars, LinkOption.NOFOLLOW_LINKS))
      throw new PackageException("Missing prepared JAR directory: " + jars);
    try (ZipFile zip = new ZipFile(archive.toFile())) {
      for (String name : source.manifest().classpath()) {
        Path path = jars.resolve(name).normalize();
        for (Path p = path; !p.equals(directory); p = p.getParent()) {
          if (Files.isSymbolicLink(p))
            throw new PackageException("Symbolic link in prepared cache: " + p);
        }
        ArchiveSupport.regular(path);
        ZipEntry entry = zip.getEntry(name);
        if (entry == null) throw new PackageException("Cached archive is missing " + name);
        try (InputStream original = zip.getInputStream(entry);
            InputStream extracted = Files.newInputStream(path)) {
          byte[] a = new byte[64 * 1024], b = new byte[64 * 1024];
          int count;
          while ((count = ArchiveSupport.readNBytes(original, a, a.length)) != 0) {
            if (ArchiveSupport.readNBytes(extracted, b, count) != count
                || !ArchiveSupport.equalBytes(a, b, count))
              throw new PackageException("Corrupt prepared JAR: " + path);
          }
          if (extracted.read() != -1)
            throw new PackageException("Corrupt prepared JAR length: " + path);
        }
        expected.add(path);
        classpath.add(path);
      }
    }
    try (Stream<Path> files = Files.walk(directory)) {
      for (Path path : files.collect(Collectors.toList())) {
        if (Files.isSymbolicLink(path))
          throw new PackageException("Symbolic link in prepared cache: " + path);
        if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) && !expected.contains(path))
          throw new PackageException("Unexpected prepared cache file: " + path);
      }
    }
    return new PreparedPackage(source, directory, classpath);
  }
}
