package io.temporal.worker.packages.tooling;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Shared by validation and parent-only class loading. */
public final class ReservedNamespaces {
  private ReservedNamespaces() {}

  private static final List<String> PREFIXES =
      Collections.unmodifiableList(
          Arrays.asList(
              "java.",
              "javax.",
              "jdk.",
              "sun.",
              "com.sun.",
              "io.temporal.",
              "io.temporal.worker.packages.api.",
              "com.google.protobuf.",
              "com.google.common.",
              "io.grpc.",
              "org.slf4j.",
              "com.uber.m3.",
              "com.fasterxml.jackson.annotation.",
              "io.nexusrpc."));

  public static List<String> prefixes() {
    return PREFIXES;
  }

  public static boolean isReserved(String binaryName) {
    return PREFIXES.stream().anyMatch(binaryName::startsWith);
  }
}
