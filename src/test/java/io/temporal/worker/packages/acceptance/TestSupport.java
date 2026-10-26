package io.temporal.worker.packages.acceptance;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;

/** Java 8 helpers shared by the test harness and its fixture builders. */
final class TestSupport {
  private TestSupport() {}

  static String readString(Path path) throws IOException {
    return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
  }

  static Path writeString(Path path, CharSequence text, OpenOption... options) throws IOException {
    return Files.write(path, text.toString().getBytes(StandardCharsets.UTF_8), options);
  }

  static byte[] readAllBytes(InputStream input) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int count;
    while ((count = input.read(buffer)) != -1) output.write(buffer, 0, count);
    return output.toByteArray();
  }

  static String repeat(String text, int count) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < count; i++) result.append(text);
    return result.toString();
  }

  @SuppressWarnings("unchecked")
  static <K, V> Map<K, V> map(Object... entries) {
    Map<K, V> result = new LinkedHashMap<>();
    for (int i = 0; i < entries.length; i += 2) result.put((K) entries[i], (V) entries[i + 1]);
    return Collections.unmodifiableMap(result);
  }

  @SafeVarargs
  static <T> Set<T> set(T... values) {
    Set<T> result = new LinkedHashSet<>();
    Collections.addAll(result, values);
    return Collections.unmodifiableSet(result);
  }

  static <T> List<T> copyList(Collection<? extends T> values) {
    return Collections.unmodifiableList(new ArrayList<>(values));
  }

  static <T> T assertInstanceOf(Class<T> type, Object value) {
    Assert.assertTrue("Expected " + type.getName() + ", found " + value, type.isInstance(value));
    return type.cast(value);
  }
}
