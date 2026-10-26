package io.temporal.worker.packages.acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.worker.packages.tooling.PackagePackager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.tools.ToolProvider;

/** Compiles each fixture separately; application classes never enter the host classpath. */
public final class FixtureCompiler {
  static final String HEADER = "container.package";
  private static final ObjectMapper JSON = new ObjectMapper();

  private FixtureCompiler() {}

  public static void main(String[] args) throws Exception {
    Path destination =
        args.length == 0 ? root().resolve("build/fixtures") : java.nio.file.Paths.get(args[0]);
    for (int version : new int[] {2, 12})
      System.out.println(build(destination, version).toAbsolutePath());
  }

  static Path root() {
    Path path = java.nio.file.Paths.get(System.getProperty("plugin.rootDir", ".")).toAbsolutePath();
    while (path != null && !Files.isDirectory(path.resolve("src/test/fixtures")))
      path = path.getParent();
    if (path == null)
      throw new IllegalStateException("Cannot find plugin root; set plugin.rootDir");
    return path;
  }

  static Path build(Path target, int version) throws IOException {
    return build(target, version, "orders");
  }

  static Path build(Path target, int version, String packageId) throws IOException {
    return build(target, version, packageId, Integer.toString(version));
  }

  static Path build(Path target, int version, String packageId, String packageVersion)
      throws IOException {
    Files.createDirectories(target);
    Path source = root().resolve("src/test/fixtures");
    Path input =
        Files.createDirectories(target.resolve("input-" + packageId + "-" + packageVersion));
    Path rules =
        compileJar(
            source.resolve("rules-v" + version),
            input.resolve("lib/rules.jar"),
            java.util.Arrays.asList());
    compileJar(
        source.resolve("orders-v" + version),
        input.resolve("app/orders.jar"),
        java.util.Arrays.asList(rules));
    java.util.LinkedHashMap<String, Object> manifest =
        new java.util.LinkedHashMap<String, Object>();
    manifest.put("formatVersion", 1);
    manifest.put("containerApiVersion", 1);
    manifest.put("packageId", packageId);
    manifest.put("packageVersion", packageVersion);
    manifest.put("javaRelease", 8);
    manifest.put("payloadEncoding", "temporal-default-v1");
    manifest.put("classpath", java.util.Arrays.asList("app/orders.jar", "lib/rules.jar"));
    manifest.put(
        "workflows",
        java.util.Arrays.asList(
            entry("OrderWorkflow", "example.orders.OrderWorkflowImpl"),
            entry("PayloadWorkflow", "example.orders.PayloadWorkflowImpl"),
            entry("CancellationWorkflow", "example.orders.CancellationWorkflowImpl"),
            entry("InstanceWorkflow", "example.orders.InstanceWorkflowImpl"),
            entry("UnsupportedWorkflow", "example.orders.UnsupportedWorkflowImpl"),
            entry("DrainWorkflow", "example.orders.DrainWorkflowImpl"),
            entry("UntypedWorkflow", "example.orders.UntypedWorkflowImpl")));
    manifest.put(
        "activities",
        java.util.Arrays.asList(
            entry("ChargeCard", "example.orders.PaymentActivitiesImpl"),
            entry("EchoOrders", "example.orders.PaymentActivitiesImpl"),
            entry("Notify", "example.orders.PaymentActivitiesImpl"),
            entry("WaitForCancellation", "example.orders.PaymentActivitiesImpl"),
            entry("InstanceNumber", "example.orders.PaymentActivitiesImpl"),
            entry("Drain", "example.orders.PaymentActivitiesImpl")));
    Path manifestPath = input.resolve("workflow-package.json");
    JSON.writerWithDefaultPrettyPrinter().writeValue(manifestPath.toFile(), manifest);
    Path archive = target.resolve(packageId + "-" + packageVersion + ".twp");
    new PackagePackager().pack(manifestPath, input, archive);
    return archive;
  }

  /** Builds a command-compatible workflow/activity fix without changing the public sample. */
  static Path buildCompatiblePatch(
      Path target, String version, String patchesVersion, String behavior) throws IOException {
    Path ordinaryArchive = build(target, 2, "orders", version);
    Path input = target.resolve("input-orders-" + version);
    Path originalSources = root().resolve("src/test/fixtures/orders-v2/src/main/java");
    Path project = target.resolve("patch-source-" + version);
    Path sources = Files.createDirectories(project.resolve("src/main/java"));
    try (java.util.stream.Stream<Path> paths = Files.walk(originalSources)) {
      for (Path source :
          paths.filter(Files::isRegularFile).collect(java.util.stream.Collectors.toList())) {
        Path destination = sources.resolve(originalSources.relativize(source));
        Files.createDirectories(destination.getParent());
        Files.copy(source, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      }
    }
    Path workflowSource = sources.resolve("example/orders/OrderWorkflowImpl.java");
    String workflow = TestSupport.readString(workflowSource);
    String originalQuery = "return \"v2:\" + customer;";
    if (!workflow.contains(originalQuery)) throw new IOException("Cannot locate fixture query");
    TestSupport.writeString(
        workflowSource,
        workflow.replace(originalQuery, "return \"" + behavior + ":\" + customer;"));
    Path activitySource = sources.resolve("example/orders/PaymentActivitiesImpl.java");
    String activity = TestSupport.readString(activitySource);
    String originalResult = "return Rules.describe(input.customer, count);";
    if (!activity.contains(originalResult))
      throw new IOException("Cannot locate fixture activity result");
    TestSupport.writeString(
        activitySource,
        activity.replace(
            originalResult,
            "return \"" + behavior + ":\" + Rules.describe(input.customer, count);"));
    compileJar(
        project,
        input.resolve("app/orders.jar"),
        java.util.Arrays.asList(input.resolve("lib/rules.jar")));
    Path manifestPath = input.resolve("workflow-package.json");
    com.fasterxml.jackson.databind.node.ObjectNode manifest =
        (com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(manifestPath.toFile());
    manifest.put("patchesVersion", patchesVersion);
    JSON.writerWithDefaultPrettyPrinter().writeValue(manifestPath.toFile(), manifest);
    Path archive = target.resolve("orders-" + patchesVersion + "-" + version + ".twp");
    new PackagePackager().pack(manifestPath, input, archive);
    Files.delete(ordinaryArchive);
    return archive;
  }

  static Path buildPrivateJackson(Path target, int version) throws IOException {
    build(target, version);
    String paths = System.getProperty("fixture.jacksonJars");
    if (paths == null || paths.trim().isEmpty())
      throw new IllegalStateException(
          "Gradle privateJackson fixture dependencies were not supplied");
    Path input = target.resolve("input-orders-" + version);
    com.fasterxml.jackson.databind.node.ObjectNode manifest =
        (com.fasterxml.jackson.databind.node.ObjectNode)
            JSON.readTree(input.resolve("workflow-package.json").toFile());
    com.fasterxml.jackson.databind.node.ArrayNode classpath =
        (com.fasterxml.jackson.databind.node.ArrayNode) manifest.get("classpath");
    for (String jar : paths.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
      Path source = java.nio.file.Paths.get(jar);
      String relative = "lib/" + source.getFileName();
      Files.copy(
          source, input.resolve(relative), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      classpath.add(relative);
    }
    ((com.fasterxml.jackson.databind.node.ArrayNode) manifest.get("workflows"))
        .add(JSON.valueToTree(entry("JacksonWorkflow", "example.orders.JacksonWorkflowImpl")));
    Path manifestPath = input.resolve("workflow-package.json");
    JSON.writerWithDefaultPrettyPrinter().writeValue(manifestPath.toFile(), manifest);
    Path archive = target.resolve("orders-private-jackson-" + version + ".twp");
    new PackagePackager().pack(manifestPath, input, archive);
    return archive;
  }

  private static Map<String, String> entry(String type, String implementation) {
    return TestSupport.map("type", type, "implementation", implementation);
  }

  static Path compileJar(Path project, Path jar, List<Path> extraClasspath) throws IOException {
    Path classes = Files.createDirectories(jar.getParent().resolve("classes-" + jar.getFileName()));
    javax.tools.JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) throw new IllegalStateException("A JDK is required, not a JRE");
    List<String> args = new ArrayList<>();
    if (!"1.8".equals(System.getProperty("java.specification.version"))) {
      args.addAll(java.util.Arrays.asList("--release", "8"));
    } else {
      args.addAll(java.util.Arrays.asList("-source", "8", "-target", "8"));
    }
    args.addAll(
        java.util.Arrays.asList(
            "-Xlint:none",
            "-Xlint:deprecation",
            "-Werror",
            "-parameters",
            "-d",
            classes.toString(),
            "-classpath"));
    String classpath =
        System.getProperty("acceptance.classpath", System.getProperty("java.class.path"));
    for (Path extra : extraClasspath) classpath += java.io.File.pathSeparator + extra;
    args.add(classpath);
    try (java.util.stream.Stream<Path> paths = Files.walk(project.resolve("src/main/java"))) {
      paths
          .filter(p -> p.toString().endsWith(".java"))
          .sorted()
          .forEach(p -> args.add(p.toString()));
    }
    if (compiler.run(null, null, null, args.toArray(new String[0])) != 0)
      throw new IOException("Fixture compilation failed: " + project);
    Files.createDirectories(jar.getParent());
    try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
      addTree(output, classes);
      Path resources = project.resolve("src/main/resources");
      if (Files.isDirectory(resources)) addTree(output, resources);
    }
    return jar;
  }

  private static void addTree(JarOutputStream output, Path tree) throws IOException {
    try (java.util.stream.Stream<Path> paths = Files.walk(tree)) {
      for (Path file :
          paths
              .filter(Files::isRegularFile)
              .sorted(Comparator.comparing(Path::toString))
              .collect(java.util.stream.Collectors.toList())) {
        JarEntry entry = new JarEntry(tree.relativize(file).toString().replace('\\', '/'));
        entry.setTime(0);
        output.putNextEntry(entry);
        Files.copy(file, output);
        output.closeEntry();
      }
    }
  }
}
