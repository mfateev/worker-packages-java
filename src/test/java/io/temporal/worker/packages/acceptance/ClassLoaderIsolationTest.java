package io.temporal.worker.packages.acceptance;

import static org.junit.Assert.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.m3.tally.Scope;
import io.temporal.worker.packages.runtime.PreparedPackageRuntime;
import io.temporal.workflow.Workflow;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ClassLoaderIsolationTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  Path temp;

  @Before
  public void initializeTemporaryFolder() {
    temp = temporaryFolder.getRoot().toPath();
  }

  @Test
  public void identicalClassesHavePrivateDependenciesAndResourcesButShareSdkBoundary()
      throws Exception {
    System.clearProperty("fixture.rules.initialized.v2");
    System.clearProperty("fixture.rules.initialized.v12");
    Path fixtures = temp.resolve("fixtures");
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(fixtures, 2), packages.resolve("old.twp"));
    Files.copy(FixtureCompiler.build(fixtures, 12), packages.resolve("new.twp"));
    assertThrows(
        ClassNotFoundException.class, () -> Class.forName("example.orders.OrderWorkflowImpl"));
    assertThrows(ClassNotFoundException.class, () -> Class.forName("example.rules.Rules"));
    try (PreparedPackageRuntime runtime =
        PreparedPackageRuntime.open(packages, temp.resolve("cache"))) {
      assertNull(
          "archive validation and preparation do not initialize application classes",
          System.getProperty("fixture.rules.initialized.v2"));
      assertNull(System.getProperty("fixture.rules.initialized.v12"));
      ClassLoader old = runtime.classLoader(runtime.catalog().require("orders", 2).ref());
      ClassLoader current = runtime.classLoader(runtime.catalog().require("orders", 12).ref());
      assertSame(old, runtime.classLoader(runtime.catalog().require("orders", 2).ref()));
      assertNotSame(old, current);
      assertNotSame(
          old.loadClass("example.orders.OrderWorkflowImpl"),
          current.loadClass("example.orders.OrderWorkflowImpl"));
      assertNotSame(old.loadClass("example.rules.Rules"), current.loadClass("example.rules.Rules"));
      assertEquals(
          "rules-v2:A:3",
          old.loadClass("example.rules.Rules")
              .getMethod("describe", String.class, int.class)
              .invoke(null, "A", 3));
      Class<?> rules = current.loadClass("example.rules.Rules");
      assertThrows(
          NoSuchMethodException.class, () -> rules.getMethod("describe", String.class, int.class));
      assertEquals(
          "rules-v12:B:3",
          rules
              .getMethod("describe", int.class, String.class)
              .invoke(rules.getConstructor().newInstance(), 3, "B"));
      for (ClassLoader loader : new ClassLoader[] {old, current}) {
        assertSame(Workflow.class, loader.loadClass(Workflow.class.getName()));
        assertSame(Scope.class, loader.loadClass(Scope.class.getName()));
        assertSame(JsonProperty.class, loader.loadClass(JsonProperty.class.getName()));
        assertEquals(1, Collections.list(loader.getResources("rules-version.txt")).size());
        assertEquals(
            1,
            Collections.list(loader.getResources("META-INF/services/example.rules.RuleService"))
                .size());
        Class<?> serviceType = loader.loadClass("example.rules.RuleService");
        Object provider = java.util.ServiceLoader.load(serviceType, loader).iterator().next();
        assertSame(loader, provider.getClass().getClassLoader());
        assertEquals(
            loader == old ? "rules-v2" : "rules-v12",
            serviceType.getMethod("identity").invoke(provider));
      }
      try (java.io.InputStream stream = old.getResourceAsStream("rules-version.txt")) {
        assertEquals("rules-v2", new String(TestSupport.readAllBytes(stream)).trim());
      }
      try (java.io.InputStream stream = current.getResourceAsStream("rules-version.txt")) {
        assertEquals("rules-v12", new String(TestSupport.readAllBytes(stream)).trim());
      }
    }
  }
}
