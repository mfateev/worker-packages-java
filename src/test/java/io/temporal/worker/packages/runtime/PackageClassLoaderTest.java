package io.temporal.worker.packages.runtime;

import static org.junit.Assert.*;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.tools.ToolProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PackageClassLoaderTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private Path temporary;

  @Before
  public void createTemporaryDirectory() {
    temporary = temporaryFolder.getRoot().toPath();
  }

  @Test
  public void identicalNamesRemainPrivateAndResourcesAreOrdered() throws Exception {
    Path two = jar("two", "v2");
    Path twelve = jar("twelve", "v12");
    try (io.temporal.worker.packages.runtime.PackageClassLoader a =
            new PackageClassLoader(new URL[] {two.toUri().toURL()}, getClass().getClassLoader());
        io.temporal.worker.packages.runtime.PackageClassLoader b =
            new PackageClassLoader(
                new URL[] {twelve.toUri().toURL()}, getClass().getClassLoader())) {
      Class<?> first = a.loadClass("isolated.Rules");
      Class<?> second = b.loadClass("isolated.Rules");
      assertNotSame(first, second);
      assertEquals("v2", first.getMethod("value").invoke(first.getConstructor().newInstance()));
      assertEquals("v12", second.getMethod("value").invoke(second.getConstructor().newInstance()));
      assertSame(String.class, a.loadClass("java.lang.String"));
      assertSame(
          io.temporal.worker.packages.api.PackageRef.class,
          a.loadClass("io.temporal.worker.packages.api.PackageRef"));
      assertEquals(
          "v2",
          new String(
              TestSupport.readAllBytes(a.getResourceAsStream("fixture/value.txt")),
              StandardCharsets.UTF_8));
      java.util.ArrayList<java.net.URL> urls =
          Collections.list(a.getResources("fixture/value.txt"));
      assertEquals(1, urls.size());
      assertEquals(a.getResource("fixture/value.txt"), urls.get(0));
      java.util.ArrayList<java.net.URL> services =
          Collections.list(a.getResources("META-INF/services/fixture.Service"));
      assertEquals(1, services.size());
      assertTrue(services.get(0).toString().contains(two.getFileName().toString()));
    }
  }

  @Test
  public void sharedNamespacesNeverFallBackToBundledClasses() throws Exception {
    Path jar = jar("shadow", "value");
    try (io.temporal.worker.packages.runtime.PackageClassLoader loader =
        new PackageClassLoader(new URL[] {jar.toUri().toURL()}, getClass().getClassLoader())) {
      assertThrows(
          ClassNotFoundException.class, () -> loader.loadClass("io.temporal.missing.BundleOnly"));
      assertThrows(
          ClassNotFoundException.class,
          () -> loader.loadClass("io.temporal.worker.packages.api.BundleOnly"));
    }
  }

  private Path jar(String directory, String value) throws Exception {
    Path root = Files.createDirectories(temporary.resolve(directory));
    Path source = root.resolve("src/isolated/Rules.java");
    Files.createDirectories(source.getParent());
    TestSupport.writeString(
        source,
        "package isolated; public final class Rules { public String value() { return \""
            + value
            + "\"; } }");
    Path output = Files.createDirectory(root.resolve("classes"));
    assertEquals(
        0,
        ToolProvider.getSystemJavaCompiler()
            .run(
                null,
                null,
                null,
                "-source",
                "8",
                "-target",
                "8",
                "-d",
                output.toString(),
                source.toString()));
    Path result = root.resolve(directory + ".jar");
    try (JarOutputStream zip = new JarOutputStream(Files.newOutputStream(result))) {
      put(zip, "isolated/Rules.class", Files.readAllBytes(output.resolve("isolated/Rules.class")));
      put(zip, "fixture/value.txt", value.getBytes(StandardCharsets.UTF_8));
      put(
          zip,
          "META-INF/services/fixture.Service",
          "isolated.Rules".getBytes(StandardCharsets.UTF_8));
      // Invalid bytecode here is deliberate: parent-only lookup must not even try to define it.
      put(zip, "io/temporal/missing/BundleOnly.class", new byte[] {0});
      put(zip, "io/temporal/worker/packages/api/BundleOnly.class", new byte[] {0});
    }
    return result;
  }

  private static void put(JarOutputStream zip, String name, byte[] bytes) throws Exception {
    zip.putNextEntry(new JarEntry(name));
    zip.write(bytes);
    zip.closeEntry();
  }
}
