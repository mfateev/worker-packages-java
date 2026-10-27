package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;
import static io.temporal.worker.packages.acceptance.TestSupport.assertInstanceOf;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.api.common.v1.Payload;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowUpdateException;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.interceptors.*;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.packages.tooling.PackageCatalog;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

public class VersionIsolationTest {
  @Rule public Timeout timeout = Timeout.seconds(120);
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  Path temp;

  @Before
  public void initializeTemporaryFolder() {
    temp = temporaryFolder.getRoot().toPath();
  }

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  public void oldRunSurvivesColdRestartAndNewRunUsesCurrentWithHeadersAndHandlers()
      throws Exception {
    Path fixtures = temp.resolve("fixtures");
    Path oldArchive = FixtureCompiler.build(fixtures, 2);
    Path newArchive = FixtureCompiler.build(fixtures, 12);
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(oldArchive, packages.resolve("z-old.twp"));
    String queue = "isolation-" + UUID.randomUUID();
    AtomicInteger observed = new AtomicInteger();
    Payload tracing =
        Payload.newBuilder()
            .putMetadata("encoding", com.google.protobuf.ByteString.copyFromUtf8("binary/plain"))
            .setData(com.google.protobuf.ByteString.copyFromUtf8("trace-fixture-opaque"))
            .build();
    WorkerInterceptor observer =
        new WorkerInterceptorBase() {
          @Override
          public ActivityInboundCallsInterceptor interceptActivity(
              ActivityInboundCallsInterceptor next) {
            return new ActivityInboundCallsInterceptorBase(next) {
              @Override
              public ActivityOutput execute(ActivityInput input) {
                assertEquals(tracing, input.getHeader().getValues().get("traceparent"));
                assertTrue(input.getHeader().getValues().containsKey(FixtureCompiler.HEADER));
                return super.execute(input);
              }
            };
          }

          @Override
          public WorkflowInboundCallsInterceptor interceptWorkflow(
              WorkflowInboundCallsInterceptor next) {
            return new WorkflowInboundCallsInterceptorBase(next) {
              @Override
              public void init(WorkflowOutboundCallsInterceptor outbound) {
                super.init(
                    new WorkflowOutboundCallsInterceptorBase(outbound) {
                      private int sequence;

                      @Override
                      public <R> ActivityOutput<R> executeActivity(ActivityInput<R> input) {
                        java.util.HashMap<String, Payload> headers =
                            new java.util.HashMap<>(input.getHeader().getValues());
                        headers.put("traceparent", tracing);
                        return super.executeActivity(
                            new ActivityInput<>(
                                input.getActivityName(),
                                input.getActivityId() == null
                                    ? "explicit-" + input.getActivityName() + "-" + (++sequence)
                                    : input.getActivityId(),
                                input.getResultClass(),
                                input.getResultType(),
                                input.getArgs(),
                                input.getOptions(),
                                new Header(headers)));
                      }
                    });
              }

              @Override
              public WorkflowOutput execute(WorkflowInput input) {
                assertFalse(input.getHeader().getValues().containsKey(FixtureCompiler.HEADER));
                observed.incrementAndGet();
                return super.execute(input);
              }
            };
          }
        };
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      WorkflowStub a = workflow(client, queue, "old-" + UUID.randomUUID(), "OrderWorkflow");
      try (Host host = host(client, packages, temp.resolve("cache-a"), queue, observer)) {
        host.start();
        a.start(new ClientOrderInput("Ada"), true, false);
        await(
            "first successful task and query",
            Duration.ofSeconds(15),
            () -> a.query("state", String.class).equals("v2:Ada"));
        assertThrows(
            WorkflowUpdateException.class, () -> a.update("rename", String.class, input("")));
        assertEquals("v2:Grace", a.update("rename", String.class, new ClientOrderInput("Grace")));
        assertEquals(TestSupport.map("customer_name", "Grace"), a.query("details", Map.class));
        long timersBefore =
            client.fetchHistory(a.getExecution().getWorkflowId()).getEvents().stream()
                .filter(e -> e.getEventType() == EventType.EVENT_TYPE_TIMER_STARTED)
                .count();
        assertThrows(
            io.temporal.client.WorkflowQueryException.class,
            () -> a.query("badQuery", String.class));
        assertEquals(
            timersBefore,
            client.fetchHistory(a.getExecution().getWorkflowId()).getEvents().stream()
                .filter(e -> e.getEventType() == EventType.EVENT_TYPE_TIMER_STARTED)
                .count());
        assertEquals(
            "query rejection leaves workflow healthy", "v2:Grace", a.query("state", String.class));
        assertEquals("rules-v2:Before:1", a.update("chargeNow", String.class, input("Before")));
        assertThrows(
            io.temporal.worker.packages.runtime.PackageRuntimeException.class,
            () -> host.plugin.closeAfterTermination(host.factory));
      }
      Files.copy(newArchive, packages.resolve("a-current.twp"));
      WorkflowExecutionHistory history;
      try (Host host = host(client, packages, temp.resolve("cache-b"), queue, observer)) {
        host.start();
        await(
            "reconstructed query on old version",
            Duration.ofSeconds(15),
            () -> a.query("state", String.class).equals("v2:Grace"));
        assertEquals("v2:Lin", a.update("rename", String.class, new ClientOrderInput("Lin")));
        assertEquals(
            TestSupport.map("customer_name", "Lin"),
            a.update("renameDetails", Map.class, new ClientOrderInput("Lin")));
        assertEquals(TestSupport.map("customer_name", "Lin"), a.query("details", Map.class));
        assertEquals("rules-v2:After:1", a.update("chargeNow", String.class, input("After")));
        WorkflowStub b = workflow(client, queue, "new-" + UUID.randomUUID(), "OrderWorkflow");
        b.start(input("New"), false, false);
        a.signal("proceed", new ClientOrderInput("Old"));
        assertEquals(
            "workflow-v2|rules-v2:Old:3|resource=rules-v2",
            a.getResult(20, TimeUnit.SECONDS, String.class));
        assertEquals(
            "workflow-v12|rules-v12:New:3|resource=rules-v12",
            b.getResult(20, TimeUnit.SECONDS, String.class));
        history = client.fetchHistory(a.getExecution().getWorkflowId());
        io.temporal.api.history.v1.ActivityTaskScheduledEventAttributes scheduled =
            history.getEvents().stream()
                .filter(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected matching history event"))
                .getActivityTaskScheduledEventAttributes();
        assertEquals("explicit-ChargeCard-1", scheduled.getActivityId());
        assertEquals(10, scheduled.getStartToCloseTimeout().getSeconds());
        assertEquals(5, scheduled.getHeartbeatTimeout().getSeconds());
        assertEquals(10, scheduled.getRetryPolicy().getInitialInterval().getSeconds());
        assertEquals(3, scheduled.getRetryPolicy().getMaximumAttempts());
        assertEquals(tracing, scheduled.getHeader().getFieldsOrThrow("traceparent"));
        assertDtoPayloads(history.getHistory());
        assertRouting(
            history, 2, PackageCatalog.scan(packages).require("orders", 2).ref().sha256());
        assertRouting(
            client.fetchHistory(b.getExecution().getWorkflowId()),
            12,
            PackageCatalog.scan(packages).require("orders", 12).ref().sha256());
      }
      assertTrue(
          "preexisting interceptor survives plugin installation and reconstruction",
          observed.get() >= 3);
      Path saved = temp.resolve("history.json");
      TestSupport.writeString(saved, history.toJson(true));
      try (Host replay = host(client, packages, temp.resolve("cache-replay"), queue, observer)) {
        replay.worker.replayWorkflowExecution(
            WorkflowExecutionHistory.fromJson(TestSupport.readString(saved)));
      }
    }
  }

  @Test
  public void localActivitiesGenericDtosVoidAndBufferedSignalUseUnchangedTypes() throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(temp.resolve("fixtures"), 2), packages.resolve("orders.twp"));
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      String queue = "payloads-" + UUID.randomUUID();
      WorkflowExecutionHistory localHistory;
      try (Host host = host(client, packages, temp.resolve("cache"), queue)) {
        host.start();
        ClientOrderWorkflow typed =
            client.newWorkflowStub(
                ClientOrderWorkflow.class,
                io.temporal.client.WorkflowOptions.newBuilder()
                    .setTaskQueue(queue)
                    .setWorkflowId("typed-" + UUID.randomUUID())
                    .build());
        WorkflowClient.start(typed::execute, input("Typed"), false, true);
        assertTrue(
            WorkflowStub.fromTyped(typed)
                .getResult(20, TimeUnit.SECONDS, String.class)
                .contains("workflow-v2|rules-v2:Typed"));
        WorkflowStub buffered =
            workflow(client, queue, "buffered-" + UUID.randomUUID(), "OrderWorkflow");
        buffered.signalWithStart(
            "proceed",
            new Object[] {input("Buffered")},
            new Object[] {input("Initial"), true, true});
        assertEquals(
            "workflow-v2|rules-v2:Buffered:3|resource=rules-v2",
            buffered.getResult(20, TimeUnit.SECONDS, String.class));
        localHistory = client.fetchHistory(buffered.getExecution().getWorkflowId());
        assertFalse(
            localHistory.getEvents().stream()
                .anyMatch(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED));
        for (boolean local : java.util.Arrays.asList(false, true)) {
          WorkflowStub payloads =
              workflow(
                  client, queue, "payload-" + local + "-" + UUID.randomUUID(), "PayloadWorkflow");
          payloads.start(java.util.Arrays.asList(input("One"), input("Two")), local);
          List<?> result = payloads.getResult(20, TimeUnit.SECONDS, List.class);
          assertEquals(
              java.util.Arrays.asList(
                  TestSupport.map("customer_name", "One"), TestSupport.map("customer_name", "Two")),
              result);
          WorkflowExecutionHistory history =
              client.fetchHistory(payloads.getExecution().getWorkflowId());
          HistoryEvent completed =
              history.getEvents().stream()
                  .filter(
                      e -> e.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED)
                  .findFirst()
                  .orElseThrow(() -> new AssertionError("Expected matching history event"));
          String json =
              completed
                  .getWorkflowExecutionCompletedEventAttributes()
                  .getResult()
                  .getPayloads(0)
                  .getData()
                  .toStringUtf8();
          assertTrue(json.contains("customer_name"));
          assertFalse(json.contains("secret"));
          assertFalse(json.contains("must-not-encode"));
        }
      }
      try (Host replay = host(client, packages, temp.resolve("cold-local-replay"), queue)) {
        replay.worker.replayWorkflowExecution(localHistory);
      }
    }
  }

  @Test
  public void oneGlobalCurrentVersionPreservesExistingRunsAcrossPackageIds() throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Path fixtures = temp.resolve("fixtures");
    Files.copy(FixtureCompiler.build(fixtures, 2, "orders"), packages.resolve("orders.twp"));
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient orders = client(env.getWorkflowServiceStubs());
      WorkflowClient billing = client(env.getWorkflowServiceStubs());
      String queue = "same-name-" + UUID.randomUUID();
      WorkflowStub a = workflow(orders, queue, "orders-" + UUID.randomUUID(), "OrderWorkflow");
      try (Host first = host(orders, packages, temp.resolve("cache-original"), queue)) {
        first.start();
        a.start(input("A"), true, false);
        await(
            "original version pinned",
            Duration.ofSeconds(15),
            () -> a.query("state", String.class).equals("v2:A"));
      }
      Files.copy(FixtureCompiler.build(fixtures, 12, "billing"), packages.resolve("billing.twp"));
      try (Host current = host(billing, packages, temp.resolve("cache-current"), queue)) {
        current.start();
        assertEquals(
            "2", current.plugin.runtime().catalog().require("orders", "2").ref().packageVersion());
        assertEquals(
            "12",
            current.plugin.runtime().catalog().require("billing", "12").ref().packageVersion());
        WorkflowStub b = workflow(billing, queue, "billing-" + UUID.randomUUID(), "OrderWorkflow");
        b.start(input("B"), false, true);
        a.signal("proceed", input("A"));
        assertTrue(
            a.getResult(20, TimeUnit.SECONDS, String.class).startsWith("workflow-v2|rules-v2:"));
        assertTrue(
            b.getResult(20, TimeUnit.SECONDS, String.class).startsWith("workflow-v12|rules-v12:"));
      }
    }
  }

  @Test
  public void missingAndReplacedArchivesFailReplayAndRestorationRecovers() throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Path old = FixtureCompiler.build(temp.resolve("fixtures"), 2);
    Path current = FixtureCompiler.build(temp.resolve("fixtures"), 12);
    Files.copy(old, packages.resolve("old.twp"));
    String queue = "recover-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      WorkflowExecutionHistory history;
      try (Host host = host(client, packages, temp.resolve("initial-cache"), queue)) {
        host.start();
        WorkflowStub stub =
            workflow(client, queue, "recovery-" + UUID.randomUUID(), "OrderWorkflow");
        stub.start(input("Original"), false, false);
        assertTrue(stub.getResult(20, TimeUnit.SECONDS, String.class).contains("rules-v2"));
        history = client.fetchHistory(stub.getExecution().getWorkflowId());
      }
      Files.delete(packages.resolve("old.twp"));
      Files.copy(current, packages.resolve("current.twp"));
      System.clearProperty("fixture.workflow.invoked.v2");
      System.clearProperty("fixture.workflow.invoked.v12");
      try (Host replay = host(client, packages, temp.resolve("missing-cache"), queue)) {
        Exception error =
            assertThrows(Exception.class, () -> replay.worker.replayWorkflowExecution(history));
        assertTrue(
            failureText(error),
            failureText(error).contains("Missing workflow type OrderWorkflow for version 2"));
        assertNull(System.getProperty("fixture.workflow.invoked.v2"));
        assertNull(System.getProperty("fixture.workflow.invoked.v12"));
      }
      // A different ZIP timestamp changes the full archive digest while leaving valid application
      // code.
      Path replacement = temp.resolve("replacement.twp");
      rewriteZipTimestamp(old, replacement);
      Files.copy(replacement, packages.resolve("old.twp"));
      try (Host replay = host(client, packages, temp.resolve("replaced-cache"), queue)) {
        Exception error =
            assertThrows(Exception.class, () -> replay.worker.replayWorkflowExecution(history));
        assertTrue(failureText(error), failureText(error).contains("digest mismatch"));
        assertNull(
            "replacement workflow constructor was not invoked",
            System.getProperty("fixture.workflow.invoked.v2"));
        assertNull(System.getProperty("fixture.workflow.invoked.v12"));
      }
      Files.copy(
          old, packages.resolve("old.twp"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      try (Host replay = host(client, packages, temp.resolve("restored-cache"), queue)) {
        replay.worker.replayWorkflowExecution(history);
      }
    }
  }

  @Test
  public void failureBeforeFirstCommitDoesNotPinAndRetryCanSelectNewCurrent() throws Exception {
    Path fixtures = temp.resolve("fixtures");
    FixtureCompiler.build(fixtures, 2);
    Path inputRoot = fixtures.resolve("input-orders-2");
    Path manifest = inputRoot.resolve("workflow-package.json");
    com.fasterxml.jackson.databind.node.ObjectNode changed =
        (com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(manifest.toFile());
    com.fasterxml.jackson.databind.node.ArrayNode workflowEntries = JSON.createArrayNode();
    workflowEntries.add(
        JSON.createObjectNode()
            .put("type", "PayloadWorkflow")
            .put("implementation", "example.orders.PayloadWorkflowImpl"));
    changed.set("workflows", workflowEntries);
    JSON.writeValue(manifest.toFile(), changed);
    Path packages = Files.createDirectories(temp.resolve("packages"));
    new io.temporal.worker.packages.tooling.PackagePackager()
        .pack(manifest, inputRoot, packages.resolve("incomplete-current.twp"));
    Path newer = FixtureCompiler.build(fixtures, 12);
    String queue = "first-task-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      WorkflowStub run =
          workflow(client, queue, "uncommitted-" + UUID.randomUUID(), "OrderWorkflow");
      try (Host host = host(client, packages, temp.resolve("cache-before-commit"), queue)) {
        host.start();
        run.start(input("Recovered"), false, false);
        await(
            "workflow task failure before pin commit",
            Duration.ofSeconds(20),
            () ->
                client.fetchHistory(run.getExecution().getWorkflowId()).getEvents().stream()
                    .anyMatch(e -> e.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED));
        List<HistoryEvent> events =
            client.fetchHistory(run.getExecution().getWorkflowId()).getEvents();
        assertFalse(
            events.stream()
                .anyMatch(e -> e.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED));
        assertFalse(
            events.stream()
                .anyMatch(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED));
        assertFalse(
            events.stream()
                .anyMatch(e -> e.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED));
      }
      Files.copy(newer, packages.resolve("new-current.twp"));
      try (Host restored = host(client, packages, temp.resolve("cache-after-commit"), queue)) {
        restored.start();
        assertTrue(
            run.getResult(30, TimeUnit.SECONDS, String.class)
                .startsWith("workflow-v12|rules-v12:"));
        assertRouting(
            client.fetchHistory(run.getExecution().getWorkflowId()),
            12,
            PackageCatalog.scan(packages).require("orders", 12).ref().sha256());
      }
    }
  }

  @Test
  public void missingDurablePackageFailsOnlyWorkflowTaskAndRestorationRecovers() throws Exception {
    Path fixtures = temp.resolve("fixtures");
    Path original = FixtureCompiler.build(fixtures, 2);
    Path newer = FixtureCompiler.build(fixtures, 12);
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(original, packages.resolve("old.twp"));
    String queue = "live-recovery-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      WorkflowStub old =
          workflow(client, queue, "durably-pinned-" + UUID.randomUUID(), "OrderWorkflow");
      try (Host host = host(client, packages, temp.resolve("cache-initial"), queue)) {
        host.start();
        old.start(input("Old"), true, false);
        await(
            "durable old pin",
            Duration.ofSeconds(15),
            () -> old.query("state", String.class).equals("v2:Old"));
      }
      Files.delete(packages.resolve("old.twp"));
      Files.copy(newer, packages.resolve("new-current.twp"));
      try (Host missing = host(client, packages, temp.resolve("cache-missing-live"), queue)) {
        missing.start();
        old.signal("proceed", input("Restored"));
        await(
            "task failure for unavailable durable package",
            Duration.ofSeconds(20),
            () ->
                client.fetchHistory(old.getExecution().getWorkflowId()).getEvents().stream()
                    .anyMatch(e -> e.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED));
        WorkflowExecutionHistory history = client.fetchHistory(old.getExecution().getWorkflowId());
        assertFalse(
            history.getEvents().stream()
                .anyMatch(e -> e.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED));
        assertFalse(
            history.getEvents().stream()
                .anyMatch(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED));
        WorkflowStub fresh =
            workflow(client, queue, "new-during-missing-" + UUID.randomUUID(), "OrderWorkflow");
        fresh.start(input("New"), false, false);
        assertTrue(
            fresh
                .getResult(20, TimeUnit.SECONDS, String.class)
                .startsWith("workflow-v12|rules-v12:"));
      }
      Files.copy(original, packages.resolve("old.twp"));
      try (Host restored = host(client, packages, temp.resolve("cache-restored-live"), queue)) {
        restored.start();
        assertEquals(
            "workflow-v2|rules-v2:Restored:3|resource=rules-v2",
            old.getResult(30, TimeUnit.SECONDS, String.class));
      }
    }
  }

  @Test
  public void businessFailuresAndHeartbeatCancellationRetainSdkSemantics() throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(temp.resolve("fixtures"), 2), packages.resolve("old.twp"));
    String queue = "failure-semantics-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      try (Host host = host(client, packages, temp.resolve("cache"), queue)) {
        host.start();
        WorkflowStub business =
            workflow(client, queue, "business-" + UUID.randomUUID(), "OrderWorkflow");
        business.start(input("fail:declined"), false, false);
        io.temporal.client.WorkflowFailedException failed =
            assertThrows(
                io.temporal.client.WorkflowFailedException.class,
                () -> business.getResult(20, TimeUnit.SECONDS, String.class));
        assertInstanceOf(io.temporal.failure.ActivityFailure.class, failed.getCause());
        io.temporal.failure.ApplicationFailure reason =
            assertInstanceOf(
                io.temporal.failure.ApplicationFailure.class, failed.getCause().getCause());
        assertEquals("FixtureBusiness", reason.getType());
        assertTrue(reason.isNonRetryable());
        WorkflowStub cancellation =
            workflow(client, queue, "cancel-" + UUID.randomUUID(), "CancellationWorkflow");
        cancellation.start();
        await(
            "running heartbeat activity",
            Duration.ofSeconds(15),
            () ->
                cancellation.describe().getRawDescription().getPendingActivitiesList().stream()
                    .anyMatch(a -> a.hasHeartbeatDetails()));
        cancellation.cancel();
        io.temporal.client.WorkflowFailedException canceled =
            assertThrows(
                io.temporal.client.WorkflowFailedException.class,
                () -> cancellation.getResult(20, TimeUnit.SECONDS, String.class));
        assertInstanceOf(io.temporal.failure.CanceledFailure.class, canceled.getCause());
      }
    }
  }

  @Test
  public void concurrentFirstUseCreatesOneActivityInstancePerPackage() throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Path fixtures = temp.resolve("fixtures");
    Files.copy(FixtureCompiler.build(fixtures, 2, "orders"), packages.resolve("orders.twp"));
    Path newer = FixtureCompiler.build(fixtures, 12, "billing");
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      for (String packageId : java.util.Arrays.asList("orders", "billing")) {
        if (packageId.equals("billing")) Files.copy(newer, packages.resolve("billing.twp"));
        WorkflowClient client = client(env.getWorkflowServiceStubs());
        String queue = "singleton-" + UUID.randomUUID();
        try (Host host = host(client, packages, temp.resolve("cache-" + packageId), queue)) {
          host.start();
          List<WorkflowStub> runs = new java.util.ArrayList<>();
          for (int i = 0; i < 12; i++) {
            WorkflowStub run =
                workflow(
                    client, queue, "instances-" + i + "-" + UUID.randomUUID(), "InstanceWorkflow");
            run.start(8);
            runs.add(run);
          }
          for (WorkflowStub run : runs)
            assertEquals(
                java.util.Collections.nCopies(8, 1),
                run.getResult(30, TimeUnit.SECONDS, List.class));
        }
      }
    }
  }

  @Test
  public void childWorkflowAndContinueAsNewAreRejectedBeforeCommands() throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(temp.resolve("fixtures"), 2), packages.resolve("old.twp"));
    String queue = "unsupported-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      try (Host host = host(client, packages, temp.resolve("cache"), queue)) {
        host.start();
        for (String operation : java.util.Arrays.asList("child", "continue")) {
          WorkflowStub run =
              workflow(
                  client,
                  queue,
                  "unsupported-" + operation + "-" + UUID.randomUUID(),
                  "UnsupportedWorkflow");
          run.start(operation);
          String failure = run.getResult(20, TimeUnit.SECONDS, String.class);
          assertTrue(
              failure,
              failure.contains(
                  operation.equals("child")
                      ? "Child workflows are unsupported"
                      : "Continue-As-New is unsupported"));
          List<HistoryEvent> events =
              client.fetchHistory(run.getExecution().getWorkflowId()).getEvents();
          assertFalse(
              events.stream()
                  .anyMatch(
                      e ->
                          e.getEventType()
                              == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED));
          assertFalse(
              events.stream()
                  .anyMatch(
                      e ->
                          e.getEventType()
                              == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW));
        }
      }
    }
  }

  @Test
  public void regularAndLocalActivitiesPreserveCallerIdsOptionsAndOpaqueHeaders() throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(temp.resolve("fixtures"), 2), packages.resolve("old.twp"));
    String queue = "activity-contract-" + UUID.randomUUID();
    io.temporal.worker.packages.acceptance.ActivityContractInterceptor contract =
        new ActivityContractInterceptor();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      try (Host host = host(client, packages, temp.resolve("cache"), queue, contract)) {
        host.start();
        for (boolean local : java.util.Arrays.asList(false, true)) {
          WorkflowStub run =
              workflow(
                  client, queue, "contract-" + local + "-" + UUID.randomUUID(), "OrderWorkflow");
          run.start(input("Contract"), false, local);
          assertTrue(
              run.getResult(20, TimeUnit.SECONDS, String.class).contains("rules-v2:Contract:3"));
          List<HistoryEvent> events =
              client.fetchHistory(run.getExecution().getWorkflowId()).getEvents();
          if (local) {
            io.temporal.api.history.v1.MarkerRecordedEventAttributes marker =
                events.stream()
                    .filter(e -> e.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED)
                    .map(HistoryEvent::getMarkerRecordedEventAttributes)
                    .filter(m -> m.getMarkerName().equals("LocalActivity"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected matching history event"));
            assertEquals(
                "caller-ChargeCard",
                DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                    0,
                    java.util.Optional.of(marker.getDetailsOrThrow("activityId")),
                    String.class,
                    String.class));
            assertEquals(
                "ChargeCard",
                DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                    0,
                    java.util.Optional.of(marker.getDetailsOrThrow("type")),
                    String.class,
                    String.class));
          } else {
            io.temporal.api.history.v1.ActivityTaskScheduledEventAttributes scheduled =
                events.stream()
                    .filter(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected matching history event"))
                    .getActivityTaskScheduledEventAttributes();
            assertEquals("caller-ChargeCard", scheduled.getActivityId());
            assertEquals(10, scheduled.getStartToCloseTimeout().getSeconds());
            assertEquals(5, scheduled.getHeartbeatTimeout().getSeconds());
            assertEquals(3, scheduled.getRetryPolicy().getMaximumAttempts());
            assertEquals(contract.tracing, scheduled.getHeader().getFieldsOrThrow("traceparent"));
          }
        }
        assertEquals(2, contract.invocations.get());
      }
    }
  }

  @Test
  public void localActivitiesUseOriginalPinAfterColdRestartWithHigherCurrent() throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Path fixtures = temp.resolve("fixtures");
    Files.copy(FixtureCompiler.build(fixtures, 2), packages.resolve("old.twp"));
    Path newer = FixtureCompiler.build(fixtures, 12);
    String queue = "local-restart-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      WorkflowStub old = workflow(client, queue, "local-old-" + UUID.randomUUID(), "OrderWorkflow");
      try (Host first = host(client, packages, temp.resolve("cache-old-local"), queue)) {
        first.start();
        old.start(input("Old"), true, true);
        await(
            "old local run durably waiting",
            Duration.ofSeconds(15),
            () -> old.query("state", String.class).equals("v2:Old"));
      }
      Files.copy(newer, packages.resolve("new.twp"));
      io.temporal.worker.packages.acceptance.ActivityContractInterceptor contract =
          new ActivityContractInterceptor();
      try (Host second = host(client, packages, temp.resolve("cache-new-local"), queue, contract)) {
        second.start();
        old.signal("proceed", input("Old"));
        assertEquals(
            "workflow-v2|rules-v2:Old:3|resource=rules-v2", result(client, old, 20, String.class));
        WorkflowStub fresh =
            workflow(client, queue, "local-new-" + UUID.randomUUID(), "OrderWorkflow");
        fresh.start(input("New"), false, true);
        assertEquals(
            "workflow-v12|rules-v12:New:3|resource=rules-v12",
            fresh.getResult(20, TimeUnit.SECONDS, String.class));
        assertEquals(java.util.Arrays.asList("2", "12"), contract.packageVersions);
      }
    }
  }

  @Test
  public void untypedRegularAndLocalActivitiesUseThePinAndRejectUndeclaredExports()
      throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(temp.resolve("fixtures"), 2), packages.resolve("orders.twp"));
    String queue = "untyped-exports-" + UUID.randomUUID();
    try (TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      WorkflowClient client = client(env.getWorkflowServiceStubs());
      try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host host =
          host(client, packages, temp.resolve("cache"), queue)) {
        host.start();
        for (boolean local : java.util.Arrays.asList(false, true)) {
          WorkflowStub valid =
              workflow(
                  client,
                  queue,
                  "untyped-valid-" + local + "-" + UUID.randomUUID(),
                  "UntypedWorkflow");
          valid.start(input("Untyped"), local, "ChargeCard");
          assertEquals("rules-v2:Untyped:3", valid.getResult(20, TimeUnit.SECONDS, String.class));
          WorkflowStub rejected =
              workflow(
                  client,
                  queue,
                  "untyped-rejected-" + local + "-" + UUID.randomUUID(),
                  "UntypedWorkflow");
          rejected.start(input("Unknown"), local, "NotExported");
          assertTrue(
              rejected
                  .getResult(20, TimeUnit.SECONDS, String.class)
                  .contains("Missing activity type NotExported for version"));
          List<HistoryEvent> events =
              client.fetchHistory(rejected.getExecution().getWorkflowId()).getEvents();
          assertFalse(
              events.stream()
                  .anyMatch(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED));
          assertFalse(
              events.stream()
                  .filter(e -> e.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED)
                  .anyMatch(
                      e ->
                          e.getMarkerRecordedEventAttributes()
                              .getMarkerName()
                              .equals("LocalActivity")));
        }
      }
    }
  }

  private static void rewriteZipTimestamp(Path source, Path output) throws Exception {
    try (java.util.zip.ZipInputStream in =
            new java.util.zip.ZipInputStream(Files.newInputStream(source));
        java.util.zip.ZipOutputStream out =
            new java.util.zip.ZipOutputStream(Files.newOutputStream(output))) {
      for (java.util.zip.ZipEntry entry = in.getNextEntry();
          entry != null;
          entry = in.getNextEntry()) {
        java.util.zip.ZipEntry replacement = new java.util.zip.ZipEntry(entry.getName());
        replacement.setTime(946684800000L);
        out.putNextEntry(replacement);
        out.write(TestSupport.readAllBytes(in));
        out.closeEntry();
      }
    }
  }

  private static void assertRouting(WorkflowExecutionHistory history, int version, String digest)
      throws Exception {
    HistoryEvent started = history.getEvents().get(0);
    assertEquals(
        "OrderWorkflow",
        started.getWorkflowExecutionStartedEventAttributes().getWorkflowType().getName());
    assertFalse(
        started
            .getWorkflowExecutionStartedEventAttributes()
            .getHeader()
            .containsFields(FixtureCompiler.HEADER));
    long pinMarker = Long.MAX_VALUE;
    boolean applicationPatchObserved = false;
    for (HistoryEvent event : history.getEvents()) {
      if (event.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED) {
        io.temporal.api.history.v1.MarkerRecordedEventAttributes details =
            event.getMarkerRecordedEventAttributes();
        String marker = details.getMarkerName();
        if (marker.equals("Version")) {
          String changeId =
              DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                  0,
                  java.util.Optional.of(details.getDetailsOrThrow("changeId")),
                  String.class,
                  String.class);
          assertEquals(
              "only application patching uses SDK version markers", "application.patch", changeId);
          Integer recorded =
              DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                  0,
                  java.util.Optional.of(details.getDetailsOrThrow("version")),
                  Integer.class,
                  Integer.class);
          assertEquals("application change IDs retain SDK behavior", Integer.valueOf(3), recorded);
          applicationPatchObserved = true;
        }
        if (marker.equals("SideEffect") && pinMarker == Long.MAX_VALUE) {
          pinMarker = event.getEventId();
          String[] pin =
              DefaultDataConverter.STANDARD_INSTANCE.fromPayloads(
                  0,
                  java.util.Optional.of(details.getDetailsOrThrow("data")),
                  String[].class,
                  String[].class);
          assertArrayEquals(new String[] {Integer.toString(version), digest, "orders"}, pin);
        }
      }
      if (event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) {
        io.temporal.api.history.v1.ActivityTaskScheduledEventAttributes activity =
            event.getActivityTaskScheduledEventAttributes();
        assertEquals("ChargeCard", activity.getActivityType().getName());
        JsonNode routing = json(activity.getHeader().getFieldsOrThrow(FixtureCompiler.HEADER));
        assertEquals(1, routing.get("formatVersion").intValue());
        assertEquals("orders", routing.get("packageId").asText());
        assertTrue(routing.get("packageVersion").isTextual());
        assertEquals(Integer.toString(version), routing.get("packageVersion").textValue());
        assertEquals(digest, routing.get("sha256").asText());
        assertTrue(
            "the atomic package pin precedes activity commands", pinMarker < event.getEventId());
      }
    }
    assertNotEquals(Long.MAX_VALUE, pinMarker);
    assertTrue(applicationPatchObserved);
  }

  public static class ClientOrderInput {
    @com.fasterxml.jackson.annotation.JsonProperty("customer_name")
    public String customer;

    @com.fasterxml.jackson.annotation.JsonIgnore public String secret = "must-not-encode";

    public ClientOrderInput() {}

    public ClientOrderInput(String customer) {
      this.customer = customer;
    }
  }

  private static void assertDtoPayloads(com.google.protobuf.Message message) throws Exception {
    if (message instanceof Payload
        && ((Payload) message)
            .getMetadataOrDefault("encoding", com.google.protobuf.ByteString.EMPTY)
            .toStringUtf8()
            .equals("json/plain")) {
      Payload payload = (Payload) message;
      JsonNode value = JSON.readTree(payload.getData().toByteArray());
      if (value != null && value.isObject() && value.has("customer_name")) {
        assertFalse(value.has("customer"));
        assertFalse(value.has("secret"));
        assertFalse(payload.getData().toStringUtf8().contains("must-not-encode"));
      }
    }
    for (Object field : message.getAllFields().values()) {
      if (field instanceof com.google.protobuf.Message)
        assertDtoPayloads((com.google.protobuf.Message) field);
      else if (field instanceof List<?>) {
        List<?> items = (List<?>) field;
        for (Object item : items)
          if (item instanceof com.google.protobuf.Message)
            assertDtoPayloads((com.google.protobuf.Message) item);
      }
    }
  }

  @io.temporal.workflow.WorkflowInterface
  public interface ClientOrderWorkflow {
    @io.temporal.workflow.WorkflowMethod(name = "OrderWorkflow")
    String execute(Map<String, String> input, boolean wait, boolean local);
  }

  private static String failureText(Throwable error) {
    StringBuilder text = new StringBuilder();
    for (Throwable cause = error; cause != null; cause = cause.getCause())
      text.append(cause).append('\n');
    return text.toString();
  }

  private static JsonNode json(Payload payload) throws Exception {
    return JSON.readTree(payload.getData().toByteArray());
  }
}
