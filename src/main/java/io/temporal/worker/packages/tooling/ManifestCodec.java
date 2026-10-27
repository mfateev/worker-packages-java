package io.temporal.worker.packages.tooling;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.worker.packages.api.PackageRef;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Strict schema reader; optional legacy SDK revision metadata is accepted and ignored. */
public final class ManifestCodec {
  private static final Set<String> FIELDS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "formatVersion",
                  "containerApiVersion",
                  "packageId",
                  "packageVersion",
                  "javaRelease",
                  "payloadEncoding",
                  "classpath",
                  "workflows",
                  "activities")));
  private final ObjectMapper mapper =
      new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

  public PackageManifest read(Path path) {
    try {
      if (Files.size(path) > ValidationLimits.DEFAULT.manifestBytes())
        throw new PackageException("Manifest too large: " + path);
      return read(Files.readAllBytes(path));
    } catch (IOException e) {
      throw new PackageException("Cannot read manifest " + path, e);
    }
  }

  public PackageManifest read(byte[] bytes) {
    try {
      JsonNode n;
      try (JsonParser parser = mapper.createParser(bytes)) {
        n = mapper.readTree(parser);
        if (parser.nextToken() != null) throw new PackageException("Trailing JSON after manifest");
      }
      fields(n, FIELDS, "manifest", true);
      int format = integer(n, "formatVersion"),
          api = integer(n, "containerApiVersion"),
          release = integer(n, "javaRelease");
      if (format != 1 || api != 1 || release != 8)
        throw new PackageException("Unsupported formatVersion/containerApiVersion/javaRelease");
      String encoding = string(n, "payloadEncoding");
      if (!"temporal-default-v1".equals(encoding))
        throw new PackageException("Unsupported payloadEncoding: " + encoding);
      String packageId = logical(string(n, "packageId"));
      String version = string(n, "packageVersion");
      try {
        PackageRef.validateVersion(version);
      } catch (IllegalArgumentException invalid) {
        throw new PackageException(invalid.getMessage(), invalid);
      }
      String patchesVersion = n.has("patchesVersion") ? string(n, "patchesVersion") : null;
      if (patchesVersion != null) {
        try {
          PackageRef.validateVersion(patchesVersion);
        } catch (IllegalArgumentException invalid) {
          throw new PackageException("Invalid patchesVersion: " + invalid.getMessage(), invalid);
        }
        if (patchesVersion.equals(version))
          throw new PackageException("A package cannot patch its own version: " + version);
      }
      JsonNode cp = n.get("classpath");
      if (!cp.isArray() || cp.isEmpty())
        throw new PackageException("classpath must be a nonempty array");
      List<String> classpath = new ArrayList<>();
      Set<String> seen = new HashSet<>();
      for (JsonNode value : cp) {
        if (!value.isTextual()) throw new PackageException("classpath must contain strings");
        String path = value.textValue();
        ArchiveSupport.safeName(path, false);
        if (!path.endsWith(".jar") || !seen.add(path))
          throw new PackageException("Invalid or duplicate classpath: " + path);
        classpath.add(path);
      }
      List<PackageManifest.Entrypoint> workflows = entrypoints(n.get("workflows"), "workflows");
      List<PackageManifest.Entrypoint> activities = entrypoints(n.get("activities"), "activities");
      if (workflows.isEmpty() && activities.isEmpty())
        throw new PackageException("At least one workflow or activity is required");
      return new PackageManifest(
          format,
          api,
          packageId,
          version,
          release,
          encoding,
          classpath,
          workflows,
          activities,
          patchesVersion);
    } catch (IOException e) {
      throw new PackageException("Malformed manifest JSON: " + e.getMessage(), e);
    }
  }

  public byte[] write(PackageManifest manifest) {
    try {
      byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest);
      read(bytes);
      return bytes;
    } catch (IOException e) {
      throw new PackageException("Cannot encode manifest", e);
    }
  }

  private static List<PackageManifest.Entrypoint> entrypoints(JsonNode n, String label) {
    if (!n.isArray()) throw new PackageException(label + " must be an array");
    Set<String> types = new HashSet<>();
    List<PackageManifest.Entrypoint> result = new ArrayList<>();
    for (JsonNode item : n) {
      fields(item, new HashSet<>(Arrays.asList("type", "implementation")), label);
      String type = logical(string(item, "type")), impl = string(item, "implementation");
      if (!types.add(type)) throw new PackageException("Duplicate " + label + " type: " + type);
      if (!binaryName(impl))
        throw new PackageException("Invalid implementation binary name: " + impl);
      result.add(new PackageManifest.Entrypoint(type, impl));
    }
    return Collections.unmodifiableList(result);
  }

  private static void fields(JsonNode n, Set<String> expected, String label) {
    fields(n, expected, label, false);
  }

  private static void fields(
      JsonNode n, Set<String> expected, String label, boolean allowLegacySdkRevision) {
    if (n == null || !n.isObject()) throw new PackageException(label + " must be an object");
    Set<String> actual = new HashSet<>();
    n.fieldNames().forEachRemaining(actual::add);
    if (allowLegacySdkRevision) {
      actual.remove("temporalSdkRevision");
      actual.remove("patchesVersion");
    }
    if (!actual.equals(expected))
      throw new PackageException(
          label + " fields must be exactly " + expected + "; found " + actual);
  }

  private static String string(JsonNode n, String field) {
    JsonNode value = n.get(field);
    if (value == null || !value.isTextual())
      throw new PackageException(field + " must be a string");
    return value.textValue();
  }

  private static int integer(JsonNode n, String field) {
    JsonNode value = n.get(field);
    if (value == null || !value.isIntegralNumber() || !value.canConvertToInt())
      throw new PackageException(field + " must be a Java integer");
    return value.intValue();
  }

  public static String logical(String name) {
    if (name == null || !name.matches("[A-Za-z][A-Za-z0-9_.-]{0,99}"))
      throw new PackageException("Invalid package or logical type name: " + name);
    return name;
  }

  private static boolean binaryName(String name) {
    for (String part : name.split("\\.", -1)) {
      if (part.isEmpty() || !Character.isJavaIdentifierStart(part.charAt(0))) return false;
      for (int i = 1; i < part.length(); i++)
        if (!Character.isJavaIdentifierPart(part.charAt(i))) return false;
    }
    return true;
  }
}
