package io.temporal.worker.packages.runtime;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.api.common.v1.Payload;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.interceptors.Header;
import io.temporal.worker.packages.api.PackageRef;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

/** Wire protocol for container routing. Application argument payloads remain unchanged. */
public final class PackageHeaders {
  public static final String KEY = "container.package";
  private static final Pattern ID = Pattern.compile("[A-Za-z][A-Za-z0-9_.-]{0,99}");
  private static final Pattern DIGEST = Pattern.compile("[0-9a-f]{64}");
  private static final ObjectMapper JSON =
      new ObjectMapper(
              JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  private PackageHeaders() {}

  public static Header workflow(Header original, String packageId) {
    validId(packageId);
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("formatVersion", 1);
    metadata.put("packageId", packageId);
    return merge(original, metadata);
  }

  public static Header activity(Header original, PackageRef ref) {
    validId(ref.packageId());
    if (!DIGEST.matcher(ref.sha256()).matches()) {
      throw new PackageRuntimeException("Invalid package reference: " + ref);
    }
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("formatVersion", 1);
    metadata.put("packageId", ref.packageId());
    metadata.put("packageVersion", ref.packageVersion());
    metadata.put("sha256", ref.sha256());
    return merge(original, metadata);
  }

  public static String workflowPackageId(Header header) {
    JsonNode node = read(header, new HashSet<>(Arrays.asList("formatVersion", "packageId")));
    return node.get("packageId").textValue();
  }

  public static PackageRef activityRef(Header header) {
    JsonNode node =
        read(
            header,
            new HashSet<>(Arrays.asList("formatVersion", "packageId", "packageVersion", "sha256")));
    JsonNode version = node.get("packageVersion");
    JsonNode digest = node.get("sha256");
    if (!version.isTextual()
        || !digest.isTextual()
        || !DIGEST.matcher(digest.textValue()).matches()) {
      throw new PackageRuntimeException("Invalid package version or SHA-256 in " + KEY + " header");
    }
    try {
      return new PackageRef(
          node.get("packageId").textValue(), version.textValue(), digest.textValue());
    } catch (IllegalArgumentException failure) {
      throw new PackageRuntimeException("Invalid package version in " + KEY + " header", failure);
    }
  }

  private static Header merge(Header original, Map<String, Object> metadata) {
    Map<String, Payload> fields = new HashMap<>(original.getValues());
    // Sort metadata keys so header bytes remain stable across cold replay.
    Payload value =
        DefaultDataConverter.STANDARD_INSTANCE
            .toPayload(new java.util.TreeMap<>(metadata))
            .orElseThrow(() -> new NoSuchElementException("No value present"));
    Payload existing = fields.get(KEY);
    if (existing != null && !equivalent(existing, value)) {
      throw new PackageRuntimeException("Conflicting reserved " + KEY + " header");
    }
    fields.put(KEY, existing == null ? value : existing);
    return new Header(fields);
  }

  private static boolean equivalent(Payload left, Payload right) {
    try {
      return decode(left).equals(decode(right));
    } catch (IOException | RuntimeException e) {
      return false;
    }
  }

  private static JsonNode read(Header header, Set<String> expected) {
    Payload payload = header.getValues().get(KEY);
    if (payload == null) throw new PackageRuntimeException("Missing required " + KEY + " header");
    try {
      JsonNode node = decode(payload);
      Set<String> actual = new java.util.HashSet<>();
      node.fieldNames().forEachRemaining(actual::add);
      if (!node.isObject()
          || !actual.equals(expected)
          || !node.get("formatVersion").isIntegralNumber()
          || !node.get("formatVersion").canConvertToInt()
          || node.get("formatVersion").intValue() != 1
          || !node.get("packageId").isTextual()) {
        throw new PackageRuntimeException("Unsupported or malformed " + KEY + " header schema");
      }
      validId(node.get("packageId").textValue());
      return node;
    } catch (IOException e) {
      throw new PackageRuntimeException("Malformed " + KEY + " header JSON", e);
    }
  }

  private static JsonNode decode(Payload payload) throws IOException {
    if (!payload
        .getMetadataOrDefault("encoding", com.google.protobuf.ByteString.EMPTY)
        .toStringUtf8()
        .equals("json/plain")) {
      throw new PackageRuntimeException("Expected host JSON encoding in " + KEY + " header");
    }
    if (payload.getData().size() > 4096)
      throw new PackageRuntimeException("Oversized " + KEY + " header");
    JsonNode node = JSON.readTree(payload.getData().toByteArray());
    if (node == null) throw new PackageRuntimeException("Empty " + KEY + " header");
    return node;
  }

  static void validId(String packageId) {
    if (packageId == null || !ID.matcher(packageId).matches()) {
      throw new PackageRuntimeException("Invalid package ID: " + packageId);
    }
  }
}
