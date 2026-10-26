package io.temporal.worker.packages.runtime;

import io.temporal.worker.packages.api.PackageRef;

/** Owned by exactly one SDK workflow reconstruction, including its workflow threads. */
final class RunDispatchState {
  String packageId;
  PackageRef packageRef;
  volatile ClassLoader loader;
  boolean managed;
}
