package io.temporal.worker.packages.acceptance;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

final class ProcessHost implements AutoCloseable {
  final Process process;
  final Path log;
  private boolean closed;

  ProcessHost(String target, Path packages, Path cache, String queue, Path log) throws IOException {
    this.log = log;
    Files.createDirectories(log.getParent());
    List<String> command =
        new ArrayList<>(
            java.util.Arrays.asList(
                java.nio.file.Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dplugin.rootDir=" + FixtureCompiler.root(),
                "-cp",
                System.getProperty("acceptance.classpath", System.getProperty("java.class.path")),
                SubprocessWorker.class.getName(),
                target,
                packages.toAbsolutePath().toString(),
                cache.toAbsolutePath().toString(),
                queue));
    process =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      IntegrationSupport.await(
          "subprocess worker readiness: " + log,
          Duration.ofSeconds(30),
          () -> {
            if (!process.isAlive())
              throw new IllegalStateException(
                  "Worker exited " + process.exitValue() + ": " + readLog());
            return readLog().contains("WORKER_READY");
          });
    } catch (Throwable e) {
      process.destroyForcibly();
      throw e;
    }
  }

  private String readLog() {
    try {
      return TestSupport.readString(log);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() throws Exception {
    if (closed) return;
    try {
      if (process.isAlive()) {
        process.getOutputStream().write('\n');
        process.getOutputStream().flush();
        if (!process.waitFor(35, TimeUnit.SECONDS))
          throw new IllegalStateException("Worker shutdown timed out: " + readLog());
      }
      if (process.exitValue() != 0) throw new IllegalStateException("Worker failed: " + readLog());
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
      closed = true;
    }
  }

  static int availablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
