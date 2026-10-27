package io.temporal.worker.packages.runtime;

import io.temporal.activity.DynamicActivity;
import io.temporal.common.converter.EncodedValues;
import io.temporal.common.metadata.POJOActivityImplMetadata;
import io.temporal.common.metadata.POJOActivityMethodMetadata;
import io.temporal.common.metadata.POJOWorkflowImplMetadata;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.worker.packages.tooling.PackageCatalog;
import io.temporal.worker.packages.tooling.PackageManifest;
import io.temporal.worker.packages.tooling.PackagePreparer;
import io.temporal.worker.packages.tooling.PreparedPackage;
import io.temporal.worker.packages.tooling.ValidatedPackage;
import io.temporal.workflow.DynamicWorkflow;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.function.Supplier;

/** A fully prepared immutable catalog, with lazy isolated metadata and activity instances. */
public final class PreparedPackageRuntime implements AutoCloseable {
  private final PackageCatalog catalog;
  private final Map<String, Descriptor> descriptors;
  private volatile boolean closed;

  public static PreparedPackageRuntime open(Path packages, Path cache) throws IOException {
    return new PreparedPackageRuntime(PackageCatalog.scan(packages), new PackagePreparer(cache));
  }

  public PreparedPackageRuntime(PackageCatalog catalog, PackagePreparer preparer)
      throws IOException {
    if (catalog.isEmpty())
      throw new PackageRuntimeException("A worker runtime requires at least one package");
    this.catalog = catalog;
    Map<String, Descriptor> built = new HashMap<>();
    try {
      for (ValidatedPackage source : catalog.entries()) {
        PreparedPackage prepared = preparer.prepare(source);
        URL[] urls = new URL[prepared.classpath().size()];
        for (int i = 0; i < urls.length; i++) urls[i] = prepared.classpath().get(i).toUri().toURL();
        built.put(
            source.ref().sha256(),
            new Descriptor(
                prepared,
                new PackageClassLoader(urls, PreparedPackageRuntime.class.getClassLoader())));
      }
    } catch (Throwable failure) {
      for (Descriptor d : built.values()) {
        try {
          d.loader.close();
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
      }
      throw failure;
    }
    this.descriptors = Collections.unmodifiableMap(new HashMap<>(built));
  }

  public PackageCatalog catalog() {
    checkOpen();
    return catalog;
  }

  /** Returns the sole installed version; multi-version hosts select through plugin options. */
  public String currentVersion(String packageId) {
    checkOpen();
    return catalog.currentVersion(packageId);
  }

  public ClassLoader classLoader(PackageRef ref) {
    return descriptor(ref).loader;
  }

  void checkOpen() {
    if (closed) throw new PackageRuntimeException("Package runtime is closed");
  }

  Descriptor descriptor(PackageRef ref) {
    checkOpen();
    PackageRef effective = catalog.resolveForExecution(ref).ref();
    Descriptor found = descriptors.get(effective.sha256());
    if (found == null) throw new PackageRuntimeException("Missing prepared package " + ref);
    return found;
  }

  private Descriptor descriptor(PackageRef ref, String type, boolean workflow) {
    checkOpen();
    PackageRef effective = catalog.resolveForExecution(ref, type, workflow).ref();
    Descriptor found = descriptors.get(effective.sha256());
    if (found == null) throw new PackageRuntimeException("Missing prepared package " + ref);
    return found;
  }

  Object invokeWorkflow(
      PackageRef ref, String logicalType, EncodedValues args, RunDispatchState state) {
    Descriptor descriptor = descriptor(ref, logicalType, true);
    WorkflowEntry entry = descriptor.workflow(logicalType);
    state.loader = descriptor.loader;
    return withLoader(
        descriptor.loader,
        () -> {
          Object instance = construct(entry.constructor);
          io.temporal.workflow.Workflow.registerListener(instance);
          return invoke(entry.method, instance, args);
        });
  }

  Object invokeActivity(PackageRef ref, String logicalType, EncodedValues args) {
    Descriptor descriptor = descriptor(ref, logicalType, false);
    ActivityEntry entry = descriptor.activity(logicalType);
    return withLoader(
        descriptor.loader,
        () -> {
          FutureTask<Object> candidate = new FutureTask<>(() -> construct(entry.constructor));
          FutureTask<Object> singleton =
              descriptor.activityInstances.putIfAbsent(
                  entry.constructor.getDeclaringClass(), candidate);
          if (singleton == null) singleton = candidate;
          singleton.run();
          Object instance;
          try {
            instance = singleton.get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return rethrow(e);
          } catch (ExecutionException e) {
            descriptor.activityInstances.remove(entry.constructor.getDeclaringClass(), singleton);
            return rethrow(e.getCause());
          }
          return invoke(entry.method, instance, args);
        });
  }

  void requireActivity(PackageRef ref, String logicalType) {
    PackageManifest manifest = catalog.resolveForExecution(ref, logicalType, false).manifest();
    if (manifest.activities().stream().noneMatch(e -> e.type().equals(logicalType))) {
      throw new PackageRuntimeException("Activity " + logicalType + " is not exported by " + ref);
    }
  }

  static Object invoke(Method method, Object instance, EncodedValues args) {
    if (args.getSize() != method.getParameterCount()) {
      throw new PackageRuntimeException(
          "Wrong argument count for " + method + ": " + args.getSize());
    }
    Object[] decoded = new Object[method.getParameterCount()];
    for (int i = 0; i < decoded.length; i++) {
      decoded[i] = args.get(i, method.getParameterTypes()[i], method.getGenericParameterTypes()[i]);
    }
    try {
      return method.invoke(instance, decoded);
    } catch (InvocationTargetException e) {
      return rethrow(e.getCause());
    } catch (IllegalAccessException e) {
      throw new PackageRuntimeException("Cannot invoke " + method, e);
    }
  }

  static Object construct(Constructor<?> constructor) {
    try {
      return constructor.newInstance();
    } catch (InvocationTargetException e) {
      return rethrow(e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new PackageRuntimeException("Cannot construct " + constructor, e);
    }
  }

  static <T> T withLoader(ClassLoader loader, Supplier<T> action) {
    if (loader == null) return action.get();
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(loader);
    try {
      return action.get();
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  @SuppressWarnings("unchecked")
  static <T, E extends Throwable> T rethrow(Throwable failure) throws E {
    throw (E) failure;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) return;
    closed = true;
    IOException failure = null;
    for (Descriptor descriptor : descriptors.values()) {
      try {
        descriptor.loader.close();
      } catch (IOException e) {
        if (failure == null) failure = e;
        else failure.addSuppressed(e);
      }
    }
    if (failure != null) throw failure;
  }

  static final class Descriptor {
    final PreparedPackage prepared;
    final PackageClassLoader loader;
    final ConcurrentHashMap<String, WorkflowEntry> workflows = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, ActivityEntry> activities = new ConcurrentHashMap<>();
    final ConcurrentHashMap<Class<?>, FutureTask<Object>> activityInstances =
        new ConcurrentHashMap<>();

    Descriptor(PreparedPackage prepared, PackageClassLoader loader) {
      this.prepared = prepared;
      this.loader = loader;
    }

    WorkflowEntry workflow(String type) {
      return workflows.computeIfAbsent(
          type,
          ignored ->
              withLoader(
                  loader,
                  () -> {
                    PackageManifest.Entrypoint declaration =
                        prepared.manifest().workflows().stream()
                            .filter(e -> e.type().equals(type))
                            .findFirst()
                            .orElseThrow(
                                () ->
                                    new PackageRuntimeException(
                                        "Unknown workflow " + type + " in " + prepared.ref()));
                    Class<?> implementation = load(declaration.implementation());
                    if (DynamicWorkflow.class.isAssignableFrom(implementation)) {
                      throw new PackageRuntimeException(
                          "Dynamic application workflows are unsupported: "
                              + implementation.getName());
                    }
                    POJOWorkflowImplMetadata metadata =
                        POJOWorkflowImplMetadata.newInstance(implementation);
                    if (metadata.getWorkflowInit() != null) {
                      throw new PackageRuntimeException(
                          "@WorkflowInit is unsupported: " + implementation.getName());
                    }
                    if (metadata.getWorkflowMethods().size() != 1
                        || !metadata.getWorkflowMethods().get(0).getName().equals(type)) {
                      throw new PackageRuntimeException(
                          "Manifest workflow type must match exactly one annotated method: "
                              + type);
                    }
                    return new WorkflowEntry(
                        constructor(implementation),
                        metadata.getWorkflowMethods().get(0).getWorkflowMethod());
                  }));
    }

    ActivityEntry activity(String type) {
      return activities.computeIfAbsent(
          type,
          ignored ->
              withLoader(
                  loader,
                  () -> {
                    PackageManifest.Entrypoint declaration =
                        prepared.manifest().activities().stream()
                            .filter(e -> e.type().equals(type))
                            .findFirst()
                            .orElseThrow(
                                () ->
                                    new PackageRuntimeException(
                                        "Unknown activity " + type + " in " + prepared.ref()));
                    Class<?> implementation = load(declaration.implementation());
                    if (DynamicActivity.class.isAssignableFrom(implementation)) {
                      throw new PackageRuntimeException(
                          "Dynamic application activities are unsupported: "
                              + implementation.getName());
                    }
                    List<Method> matching = new ArrayList<>();
                    for (POJOActivityMethodMetadata method :
                        POJOActivityImplMetadata.newInstance(implementation).getActivityMethods()) {
                      if (method.getActivityTypeName().equals(type))
                        matching.add(method.getMethod());
                    }
                    if (matching.size() != 1)
                      throw new PackageRuntimeException(
                          "Manifest activity type must match exactly one annotated method: "
                              + type);
                    return new ActivityEntry(constructor(implementation), matching.get(0));
                  }));
    }

    Class<?> load(String binaryName) {
      try {
        Class<?> implementation = Class.forName(binaryName, false, loader);
        if (implementation.getClassLoader() != loader) {
          throw new PackageRuntimeException(
              "Entrypoint must be defined by its package: " + binaryName);
        }
        return implementation;
      } catch (ClassNotFoundException e) {
        throw new PackageRuntimeException("Missing entrypoint " + binaryName, e);
      }
    }

    static Constructor<?> constructor(Class<?> implementation) {
      if (!Modifier.isPublic(implementation.getModifiers())
          || Modifier.isAbstract(implementation.getModifiers())
          || implementation.isInterface()) {
        throw new PackageRuntimeException(
            "Entrypoint must be public and concrete: " + implementation.getName());
      }
      try {
        return implementation.getConstructor();
      } catch (NoSuchMethodException e) {
        throw new PackageRuntimeException(
            "Entrypoint requires a public zero-argument constructor: " + implementation.getName(),
            e);
      }
    }
  }

  private static final class WorkflowEntry {
    private final Constructor<?> constructor;
    private final Method method;

    WorkflowEntry(Constructor<?> constructor, Method method) {
      this.constructor = constructor;
      this.method = method;
    }

    public Constructor<?> constructor() {
      return constructor;
    }

    public Method method() {
      return method;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (!(other instanceof WorkflowEntry)) return false;
      WorkflowEntry that = (WorkflowEntry) other;
      return Objects.equals(constructor, that.constructor) && Objects.equals(method, that.method);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + Objects.hashCode(constructor);
      result = 31 * result + Objects.hashCode(method);
      return result;
    }

    @Override
    public String toString() {
      return "WorkflowEntry[constructor=" + constructor + ", method=" + method + "]";
    }
  }

  private static final class ActivityEntry {
    private final Constructor<?> constructor;
    private final Method method;

    ActivityEntry(Constructor<?> constructor, Method method) {
      this.constructor = constructor;
      this.method = method;
    }

    public Constructor<?> constructor() {
      return constructor;
    }

    public Method method() {
      return method;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (!(other instanceof ActivityEntry)) return false;
      ActivityEntry that = (ActivityEntry) other;
      return Objects.equals(constructor, that.constructor) && Objects.equals(method, that.method);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + Objects.hashCode(constructor);
      result = 31 * result + Objects.hashCode(method);
      return result;
    }

    @Override
    public String toString() {
      return "ActivityEntry[constructor=" + constructor + ", method=" + method + "]";
    }
  }
}
