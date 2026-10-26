package io.temporal.worker.packages.runtime;

/** An infrastructure error that fails a workflow task, allowing package restoration and retry. */
public final class PackageRuntimeException extends RuntimeException {
  public PackageRuntimeException(String message) {
    super(message);
  }

  public PackageRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
