package io.temporal.worker.packages.runtime;

import io.temporal.worker.packages.tooling.ReservedNamespaces;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;

/** One child-first loader per verified archive digest; shared APIs are parent-only. */
public final class PackageClassLoader extends URLClassLoader {
  static {
    registerAsParallelCapable();
  }

  public PackageClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    synchronized (getClassLoadingLock(name)) {
      Class<?> loaded = findLoadedClass(name);
      if (loaded == null) {
        if (ReservedNamespaces.isReserved(name)) loaded = getParent().loadClass(name);
        else {
          try {
            loaded = findClass(name);
          } catch (ClassNotFoundException absent) {
            loaded = getParent().loadClass(name);
          }
        }
      }
      if (resolve) resolveClass(loaded);
      return loaded;
    }
  }

  @Override
  public URL getResource(String name) {
    URL own = findResource(name);
    return own != null ? own : getParent().getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    LinkedHashMap<String, URL> ordered = new LinkedHashMap<>();
    Enumeration<URL> own = findResources(name);
    while (own.hasMoreElements()) {
      URL u = own.nextElement();
      ordered.putIfAbsent(u.toExternalForm(), u);
    }
    Enumeration<URL> shared = getParent().getResources(name);
    while (shared.hasMoreElements()) {
      URL u = shared.nextElement();
      ordered.putIfAbsent(u.toExternalForm(), u);
    }
    return Collections.enumeration(ordered.values());
  }
}
