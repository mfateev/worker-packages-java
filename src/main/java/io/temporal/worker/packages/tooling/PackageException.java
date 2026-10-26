package io.temporal.worker.packages.tooling;

/** Infrastructure errors remain distinct from application failures. */
public final class PackageException extends RuntimeException {
  public PackageException(String message) {
    super(message);
  }

  public PackageException(String message, Throwable cause) {
    super(message, cause);
  }
}
