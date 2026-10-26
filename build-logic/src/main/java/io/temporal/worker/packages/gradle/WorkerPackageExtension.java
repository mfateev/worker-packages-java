package io.temporal.worker.packages.gradle;

import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;

/** Declares the version and entrypoints of one independently deployable worker package. */
public abstract class WorkerPackageExtension {
  /** Returns the logical package name. */
  public abstract Property<String> getPackageId();

  /** Returns an opaque version token, defaulting to the project's version. */
  public abstract Property<String> getPackageVersion();

  /** Returns the version this patch replaces, or remains unset for a regular package. */
  public abstract Property<String> getPatchesVersion();

  /** Maps workflow type names to implementation binary class names. */
  public abstract MapProperty<String, String> getWorkflows();

  /** Maps activity type names to implementation binary class names. */
  public abstract MapProperty<String, String> getActivities();
}
