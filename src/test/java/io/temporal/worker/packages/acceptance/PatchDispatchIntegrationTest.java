package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;
import static org.junit.Assert.*;

import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowStub;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.interceptors.Header;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testserver.TestServer;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.worker.packages.runtime.PackageHeaders;
import io.temporal.worker.packages.runtime.WorkerPackagePlugin;
import io.temporal.worker.packages.tooling.PackageCatalog;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Patch dispatch overrides executable code while preserving each workflow's logical package pin.
 */
public class PatchDispatchIntegrationTest {
  private static final String BASE = "base";
  private static final String FIRST_PATCH = "fix-one";
  private static final String SECOND_PATCH = "fix-two";
  private static final String OTHER = "unrelated";
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public Timeout timeout = Timeout.seconds(180);

  @Test
  public void coldPatchOnlyChainOverridesWorkflowAndActivityWithoutChangingPinsOrOtherVersions()
      throws Exception {
    Path temp = temporaryFolder.getRoot().toPath();
    Path fixtures = temp.resolve("fixtures");
    Path base = FixtureCompiler.build(fixtures, 2, "orders", BASE);
    Path other = FixtureCompiler.build(fixtures, 12, "orders", OTHER);
    Path firstPatch = FixtureCompiler.buildCompatiblePatch(fixtures, FIRST_PATCH, BASE, "p1");
    Path secondPatch =
        FixtureCompiler.buildCompatiblePatch(fixtures, SECOND_PATCH, FIRST_PATCH, "p2");
    Path originalPackages = Files.createDirectories(temp.resolve("original-packages"));
    Files.copy(base, originalPackages.resolve(base.getFileName()));
    Files.copy(other, originalPackages.resolve(other.getFileName()));
    PackageCatalog originalCatalog = PackageCatalog.scan(originalPackages);
    PackageRef originalRef = originalCatalog.require("orders", BASE).ref();
    PackageRef unrelatedRef = originalCatalog.require("orders", OTHER).ref();
    AtomicReference<String> selected = new AtomicReference<>(BASE);
    AtomicInteger selections = new AtomicInteger();
    WorkerPackagePlugin.Options originalOptions =
        WorkerPackagePlugin.Options.newBuilder()
            .setVersionSelector(
                context -> {
                  selections.incrementAndGet();
                  return selected.get();
                })
            .build();
    AtomicInteger replaySelections = new AtomicInteger();
    WorkerPackagePlugin.Options replayOptions = forbidSelection(replaySelections);
    String queue = "patch-chain-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs(), "orders");
      WorkflowStub oldRun = workflow(client, queue, "base-" + UUID.randomUUID(), "OrderWorkflow");
      WorkflowStub unrelated =
          workflow(client, queue, "other-" + UUID.randomUUID(), "OrderWorkflow");
      try (Host original =
          host(client, originalPackages, temp.resolve("original-cache"), queue, originalOptions)) {
        original.start();
        oldRun.start(input("Old"), true, false);
        await(
            "base workflow committed",
            Duration.ofSeconds(20),
            () -> oldRun.query("state", String.class).equals("v2:Old"));
        selected.set(OTHER);
        unrelated.start(input("Other"), true, false);
        await(
            "unrelated workflow committed",
            Duration.ofSeconds(20),
            () -> unrelated.query("state", String.class).equals("v12:Other"));
        assertEquals(2, selections.get());
      }
      Path patchOnly = Files.createDirectories(temp.resolve("patch-only-packages"));
      Files.copy(firstPatch, patchOnly.resolve(firstPatch.getFileName()));
      Files.copy(secondPatch, patchOnly.resolve(secondPatch.getFileName()));
      Files.copy(other, patchOnly.resolve(other.getFileName()));
      PackageCatalog patchedCatalog = PackageCatalog.scan(patchOnly);
      assertEquals(
          new HashSet<>(Arrays.asList(FIRST_PATCH, SECOND_PATCH, OTHER)),
          patchedCatalog.entries().stream()
              .map(entry -> entry.ref().packageVersion())
              .collect(Collectors.toSet()));
      WorkflowExecutionHistory patchedHistory;
      WorkflowExecutionHistory unrelatedHistory;
      try (Host patched =
          host(client, patchOnly, temp.resolve("patched-cache"), queue, replayOptions)) {
        patched.start();
        await(
            "cold workflow dispatch reaches leaf patch",
            Duration.ofSeconds(20),
            () -> oldRun.query("state", String.class).equals("p2:Old"));
        assertEquals("v12:Other", unrelated.query("state", String.class));
        oldRun.signal("proceed", input("Old"));
        unrelated.signal("proceed", input("Other"));
        assertEquals(
            "workflow-v2|p2:rules-v2:Old:3|resource=rules-v2",
            result(client, oldRun, 25, String.class));
        assertEquals(
            "workflow-v12|rules-v12:Other:3|resource=rules-v12",
            result(client, unrelated, 25, String.class));
        patchedHistory = client.fetchHistory(oldRun.getExecution().getWorkflowId());
        unrelatedHistory = client.fetchHistory(unrelated.getExecution().getWorkflowId());
        assertPinAndHeaders(patchedHistory, originalRef);
        assertPinAndHeaders(unrelatedHistory, unrelatedRef);
        assertEquals(0, replaySelections.get());
      }
      Path savedPatched = temp.resolve("patched-history.json");
      Path savedUnrelated = temp.resolve("unrelated-history.json");
      TestSupport.writeString(savedPatched, patchedHistory.toJson(true));
      TestSupport.writeString(savedUnrelated, unrelatedHistory.toJson(true));
      try (Host replay =
          host(client, patchOnly, temp.resolve("replay-cache"), queue, replayOptions)) {
        replay.worker.replayWorkflowExecution(
            WorkflowExecutionHistory.fromJson(TestSupport.readString(savedPatched)));
        replay.worker.replayWorkflowExecution(
            WorkflowExecutionHistory.fromJson(TestSupport.readString(savedUnrelated)));
      }
      assertEquals(2, selections.get());
      assertEquals(0, replaySelections.get());
    }
  }

  @Test
  public void newWorkflowCanSelectAbsentBaseAliasAndPinTheEffectiveLeafDigest() throws Exception {
    Path temp = temporaryFolder.getRoot().toPath();
    Path fixtures = temp.resolve("fixtures");
    Path firstPatch = FixtureCompiler.buildCompatiblePatch(fixtures, FIRST_PATCH, BASE, "p1");
    Path secondPatch =
        FixtureCompiler.buildCompatiblePatch(fixtures, SECOND_PATCH, FIRST_PATCH, "p2");
    Path packages = Files.createDirectories(temp.resolve("patch-only-packages"));
    Files.copy(firstPatch, packages.resolve(firstPatch.getFileName()));
    Files.copy(secondPatch, packages.resolve(secondPatch.getFileName()));
    PackageCatalog catalog = PackageCatalog.scan(packages);
    PackageRef expected =
        new PackageRef("orders", BASE, catalog.require("orders", SECOND_PATCH).ref().sha256());
    AtomicInteger selections = new AtomicInteger();
    WorkerPackagePlugin.Options options =
        WorkerPackagePlugin.Options.newBuilder()
            .setVersionSelector(
                context -> {
                  selections.incrementAndGet();
                  assertTrue(context.getAvailableVersions().contains(BASE));
                  return BASE;
                })
            .build();
    AtomicInteger replaySelections = new AtomicInteger();
    String queue = "patch-only-new-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs(), "orders");
      WorkflowStub run = workflow(client, queue, "new-base-" + UUID.randomUUID(), "OrderWorkflow");
      WorkflowExecutionHistory history;
      try (Host host = host(client, packages, temp.resolve("cache"), queue, options)) {
        host.start();
        run.start(input("New"), false, false);
        assertEquals(
            "workflow-v2|p2:rules-v2:New:3|resource=rules-v2",
            result(client, run, 25, String.class));
        history = client.fetchHistory(run.getExecution().getWorkflowId());
        assertPinAndHeaders(history, expected);
        assertEquals(1, selections.get());
      }
      try (Host replay =
          host(
              client,
              packages,
              temp.resolve("replay-cache"),
              queue,
              forbidSelection(replaySelections))) {
        replay.worker.replayWorkflowExecution(history);
      }
      assertEquals(1, selections.get());
      assertEquals(0, replaySelections.get());
    }
  }

  @Test
  public void activityRetryInFreshJvmUsesPatchOnlyCodeWithTheOriginalHeader() throws Exception {
    Path temp = temporaryFolder.getRoot().toPath();
    Path fixtures = temp.resolve("fixtures");
    Path base = FixtureCompiler.build(fixtures, 2);
    Path patch = FixtureCompiler.buildCompatiblePatch(fixtures, "3", "2", "retry-patch");
    Path originals = Files.createDirectories(temp.resolve("original-packages"));
    Files.copy(base, originals.resolve(base.getFileName()));
    PackageRef originalRef = PackageCatalog.scan(originals).require("orders", "2").ref();
    Path patchOnly = Files.createDirectories(temp.resolve("patch-only-packages"));
    Files.copy(patch, patchOnly.resolve(patch.getFileName()));
    int port = ProcessHost.availablePort();
    String target = "127.0.0.1:" + port;
    String queue = "patch-retry-" + UUID.randomUUID();
    try (TestServer.PortBoundTestServer server = TestServer.createPortBoundServer(port, true)) {
      WorkflowServiceStubs service =
          WorkflowServiceStubs.newServiceStubs(TestServices.options().setTarget(target).build());
      try {
        WorkflowClient client = client(service, "orders");
        WorkflowStub run =
            workflow(client, queue, "retry-patch-" + UUID.randomUUID(), "OrderWorkflow");
        try (ProcessHost first =
            new ProcessHost(
                target,
                originals,
                temp.resolve("cache-first"),
                queue,
                temp.resolve("worker-first.log"))) {
          run.start(input("retry:patched"), false, false);
          await(
              "base activity fails before patch installation",
              Duration.ofSeconds(20),
              () ->
                  run.describe().getRawDescription().getPendingActivitiesList().stream()
                      .anyMatch(
                          activity ->
                              activity.hasLastFailure()
                                  && activity
                                      .getLastFailure()
                                      .getApplicationFailureInfo()
                                      .getType()
                                      .equals("FixtureRetry")));
        }
        try (ProcessHost second =
            new ProcessHost(
                target,
                patchOnly,
                temp.resolve("cache-second"),
                queue,
                temp.resolve("worker-second.log"))) {
          assertEquals(
              "workflow-v2|retry-patch:rules-v2:retry:patched:3|resource=rules-v2",
              result(client, run, 40, String.class));
        }
        assertPinAndHeaders(client.fetchHistory(run.getExecution().getWorkflowId()), originalRef);
        assertEquals(1, PackageCatalog.scan(patchOnly).entries().size());
      } finally {
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }

  private static WorkerPackagePlugin.Options forbidSelection(AtomicInteger selections) {
    return WorkerPackagePlugin.Options.newBuilder()
        .setVersionSelector(
            context -> {
              selections.incrementAndGet();
              throw new IllegalStateException(
                  "A replayed package pin must not invoke version selection");
            })
        .build();
  }

  private static void assertPinAndHeaders(WorkflowExecutionHistory history, PackageRef expected) {
    int pins = 0;
    int activities = 0;
    long pinId = Long.MAX_VALUE;
    for (HistoryEvent event : history.getEvents()) {
      if (event.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED
          && event.getMarkerRecordedEventAttributes().getMarkerName().equals("SideEffect")) {
        pins++;
        pinId = event.getEventId();
        String[] pin =
            DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                0,
                Optional.of(event.getMarkerRecordedEventAttributes().getDetailsOrThrow("data")),
                String[].class,
                String[].class);
        assertArrayEquals(new String[] {expected.packageVersion(), expected.sha256()}, pin);
      }
      if (event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) {
        activities++;
        assertTrue(pinId < event.getEventId());
        assertEquals(
            "ChargeCard",
            event.getActivityTaskScheduledEventAttributes().getActivityType().getName());
        assertEquals(
            expected,
            PackageHeaders.activityRef(
                new Header(event.getActivityTaskScheduledEventAttributes().getHeader())));
      }
    }
    assertEquals(1, pins);
    assertEquals(1, activities);
  }
}
