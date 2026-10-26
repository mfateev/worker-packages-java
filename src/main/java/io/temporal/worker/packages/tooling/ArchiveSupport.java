package io.temporal.worker.packages.tooling;

import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class ArchiveSupport {
  private ArchiveSupport() {}

  static void safeName(String name, boolean directory) {
    if (name == null
        || name.isEmpty()
        || name.startsWith("/")
        || name.contains("\\")
        || name.contains(":")
        || name.indexOf('\0') >= 0) {
      throw new PackageException("Unsafe archive path: " + name);
    }
    String clean = directory && name.endsWith("/") ? name.substring(0, name.length() - 1) : name;
    for (String part : clean.split("/", -1)) {
      if (part.isEmpty() || part.equals(".") || part.equals(".."))
        throw new PackageException("Unsafe archive path: " + name);
    }
  }

  static void regular(Path path) {
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || !Files.isReadable(path))
      throw new PackageException("Expected readable regular file: " + path);
  }

  static void noSymlinkComponents(Path path) {
    if (Files.isSymbolicLink(path))
      throw new PackageException("Symbolic link is not allowed: " + path);
  }

  static void noSymlinkComponents(Path root, Path path) {
    Path base = root.toAbsolutePath().normalize();
    Path target = path.toAbsolutePath().normalize();
    if (!target.startsWith(base)) throw new PackageException("Path escapes input root: " + path);
    noSymlinkComponents(base);
    Path current = base;
    for (Path component : base.relativize(target)) {
      current = current.resolve(component);
      noSymlinkComponents(current);
    }
  }

  static String digest(Path path) throws IOException {
    return digest(path, ValidationLimits.DEFAULT.archiveBytes());
  }

  static String digest(Path path, long maximum) throws IOException {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
    try (InputStream in = Files.newInputStream(path)) {
      byte[] buffer = new byte[64 * 1024];
      int count;
      long bytes = 0;
      while ((count = in.read(buffer)) != -1) {
        bytes += count;
        if (bytes > maximum)
          throw new PackageException("Compressed archive size limit exceeded: " + path);
        md.update(buffer, 0, count);
      }
    }
    char[] alphabet = "0123456789abcdef".toCharArray();
    byte[] digest = md.digest();
    char[] hex = new char[digest.length * 2];
    for (int i = 0; i < digest.length; i++) {
      hex[2 * i] = alphabet[(digest[i] & 0xff) >>> 4];
      hex[2 * i + 1] = alphabet[digest[i] & 0x0f];
    }
    return new String(hex);
  }

  static void deleteTree(Path path) throws IOException {
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return;
    try (Stream<Path> paths = Files.walk(path)) {
      for (Path p : paths.sorted(java.util.Comparator.reverseOrder()).collect(Collectors.toList()))
        Files.delete(p);
    }
  }

  static final OutputStream DISCARD =
      new OutputStream() {
        @Override
        public void write(int value) {}

        @Override
        public void write(byte[] buffer, int offset, int count) {}
      };

  static int readNBytes(InputStream in, byte[] buffer, int count) throws IOException {
    int bytes = 0;
    while (bytes < count) {
      int read = in.read(buffer, bytes, count - bytes);
      if (read < 0) break;
      bytes += read;
    }
    return bytes;
  }

  static boolean equalBytes(byte[] first, byte[] second, int count) {
    for (int i = 0; i < count; i++) {
      if (first[i] != second[i]) return false;
    }
    return true;
  }

  static final class Budget {
    private final ValidationLimits limits;
    private long expanded;
    private int entries;

    Budget(ValidationLimits limits) {
      this.limits = limits;
    }

    void entry() {
      if (++entries > limits.entries())
        throw new PackageException("Archive entry count limit exceeded");
    }

    long transfer(InputStream in, OutputStream out, long maximum) throws IOException {
      byte[] buffer = new byte[64 * 1024];
      long bytes = 0;
      int count;
      while ((count = in.read(buffer)) != -1) {
        bytes += count;
        expanded += count;
        if (bytes > maximum || expanded > limits.expandedBytes())
          throw new PackageException("Archive expanded size limit exceeded");
        out.write(buffer, 0, count);
      }
      return bytes;
    }
  }

  /** Read central-directory attributes because java.util.zip does not expose Unix file modes. */
  static void rejectSymlinks(Path path, int maximumEntries) throws IOException {
    try (RandomAccessFile in = new RandomAccessFile(path.toFile(), "r")) {
      long length = in.length();
      int tailLength = (int) Math.min(length, 65_557);
      byte[] tail = new byte[tailLength];
      in.seek(length - tailLength);
      in.readFully(tail);
      int eocd = -1;
      for (int i = tail.length - 22; i >= 0; i--) {
        if (u32(tail, i) == 0x06054b50L && i + 22 + u16(tail, i + 20) == tail.length) {
          eocd = i;
          break;
        }
      }
      if (eocd < 0) throw new PackageException("Missing ZIP central directory: " + path);
      long total = u16(tail, eocd + 10), offset = u32(tail, eocd + 16);
      if (u16(tail, eocd + 4) != 0 || u16(tail, eocd + 6) != 0)
        throw new PackageException("Multi-disk ZIP is unsupported");
      if (total == 0xffff || offset == 0xffffffffL) {
        long locator = length - tailLength + eocd - 20;
        if (locator < 0) throw new PackageException("Invalid ZIP64 directory");
        in.seek(locator);
        byte[] b = new byte[20];
        in.readFully(b);
        if (u32(b, 0) != 0x07064b50L) throw new PackageException("Invalid ZIP64 locator");
        in.seek(u64(b, 8));
        byte[] z = new byte[56];
        in.readFully(z);
        if (u32(z, 0) != 0x06064b50L || u32(z, 16) != 0 || u32(z, 20) != 0)
          throw new PackageException("Invalid ZIP64 directory");
        total = u64(z, 32);
        offset = u64(z, 48);
      }
      if (total < 0 || total > maximumEntries || offset < 0 || offset > length)
        throw new PackageException("Invalid ZIP central directory limits");
      in.seek(offset);
      for (long i = 0; i < total; i++) {
        byte[] header = new byte[46];
        in.readFully(header);
        if (u32(header, 0) != 0x02014b50L)
          throw new PackageException("Invalid ZIP central directory entry");
        if (((u32(header, 38) >>> 16) & 0170000) == 0120000)
          throw new PackageException("Symbolic link ZIP entry in " + path);
        long next = in.getFilePointer() + u16(header, 28) + u16(header, 30) + u16(header, 32);
        if (next > length) throw new PackageException("Truncated ZIP central directory");
        in.seek(next);
      }
    }
  }

  private static int u16(byte[] b, int p) {
    return (b[p] & 255) | ((b[p + 1] & 255) << 8);
  }

  private static long u32(byte[] b, int p) {
    return Integer.toUnsignedLong(
        (int)
            ((b[p] & 255)
                | ((b[p + 1] & 255) << 8)
                | ((b[p + 2] & 255) << 16)
                | ((b[p + 3] & 255) << 24)));
  }

  private static long u64(byte[] b, int p) {
    return u32(b, p) | (u32(b, p + 4) << 32);
  }
}
