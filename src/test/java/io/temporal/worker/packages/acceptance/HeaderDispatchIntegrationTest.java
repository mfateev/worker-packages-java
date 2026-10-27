package io.temporal.worker.packages.acceptance;

import static io.temporal.worker.packages.acceptance.IntegrationSupport.*;
import static io.temporal.worker.packages.acceptance.TestSupport.assertInstanceOf;
import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.activity.ActivityOptions;
import io.temporal.api.common.v1.Payload;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.enums.v1.RetryState;
import io.temporal.client.*;
import io.temporal.common.RetryOptions;
import io.temporal.common.SimplePlugin;
import io.temporal.common.interceptors.*;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.worker.packages.runtime.PackageHeaders;
import io.temporal.worker.packages.runtime.WorkerPackagePlugin;
import io.temporal.worker.packages.tooling.PackageCatalog;
import io.temporal.workflow.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

public class HeaderDispatchIntegrationTest {
  @Rule public Timeout timeout = Timeout.seconds(120);
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  Path temp;

  @Before
  public void initializeTemporaryFolder() {
    temp = temporaryFolder.getRoot().toPath();
  }

  @Test
  public void malformedWorkflowStartHeadersFailTasksBeforePinningOrApplicationCode()
      throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(temp.resolve("fixtures"), 2), packages.resolve("orders.twp"));
    List<InvalidHeader> invalid =
        Arrays.asList(
            new InvalidHeader("{broken", "Malformed"),
            new InvalidHeader("{}", "malformed"),
            new InvalidHeader("{\"formatVersion\":2,\"packageId\":\"orders\"}", "Unsupported"),
            new InvalidHeader(
                "{\"formatVersion\":1,\"packageId\":\"../orders\"}", "Invalid package ID"));
    try (io.temporal.testing.TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      for (InvalidHeader entry : invalid) {
        WorkflowClientInterceptorBase interceptor =
            new WorkflowClientInterceptorBase() {
              @Override
              public WorkflowClientCallsInterceptor workflowClientCallsInterceptor(
                  WorkflowClientCallsInterceptor next) {
                return new WorkflowClientCallsInterceptorBase(next) {
                  @Override
                  public WorkflowStartOutput start(WorkflowStartInput input) {
                    return super.start(
                        new WorkflowStartInput(
                            input.getWorkflowId(),
                            input.getWorkflowType(),
                            rawHeader(entry.json()),
                            input.getArguments(),
                            input.getOptions()));
                  }
                };
              }
            };
        io.temporal.client.WorkflowClient client =
            WorkflowClient.newInstance(
                env.getWorkflowServiceStubs(),
                WorkflowClientOptions.newBuilder().setInterceptors(interceptor).build());
        String queue = "bad-start-" + UUID.randomUUID();
        System.clearProperty("fixture.workflow.invoked.v2");
        try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host host =
            host(client, packages, temp.resolve(queue), queue)) {
          host.start();
          io.temporal.client.WorkflowStub run =
              workflow(client, queue, "malformed-" + UUID.randomUUID(), "OrderWorkflow");
          run.start(input("Not invoked"), false, false);
          try {
            await(
                "malformed workflow header diagnostic",
                Duration.ofSeconds(15),
                () ->
                    client.fetchHistory(run.getExecution().getWorkflowId()).getEvents().stream()
                        .filter(e -> e.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED)
                        .anyMatch(
                            e ->
                                e.getWorkflowTaskFailedEventAttributes()
                                    .getFailure()
                                    .toString()
                                    .contains(entry.diagnostic())));
            io.temporal.common.WorkflowExecutionHistory history =
                client.fetchHistory(run.getExecution().getWorkflowId());
            assertFalse(
                history.getEvents().stream()
                    .anyMatch(e -> e.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED));
            assertFalse(
                history.getEvents().stream()
                    .anyMatch(
                        e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED));
            assertFalse(
                history.getEvents().stream()
                    .anyMatch(
                        e -> e.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED));
            assertNull(System.getProperty("fixture.workflow.invoked.v2"));
          } finally {
            run.terminate("malformed-header acceptance case finished");
          }
        }
      }
    }
  }

  @Test
  public void malformedActivityHeadersReachDynamicWorkerAndFailWithoutRunningReplacementCode()
      throws Exception {
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(FixtureCompiler.build(temp.resolve("fixtures"), 12), packages.resolve("orders.twp"));
    List<InvalidHeader> invalid =
        Arrays.asList(
            new InvalidHeader(null, "Missing required"),
            new InvalidHeader("{broken", "Malformed"),
            new InvalidHeader("{\"formatVersion\":1,\"packageId\":\"orders\"}", "malformed"),
            new InvalidHeader(
                "{\"formatVersion\":1,\"packageId\":\"orders\",\"packageVersion\":12,\"sha256\":\""
                    + TestSupport.repeat("a", 64)
                    + "\"}",
                "Invalid package version"));
    try (io.temporal.testing.TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      io.temporal.client.WorkflowClient client =
          WorkflowClient.newInstance(env.getWorkflowServiceStubs());
      for (InvalidHeader entry : invalid) {
        CopyOnWriteArrayList<Integer> attempts = new CopyOnWriteArrayList<Integer>();
        String queue = "bad-activity-" + UUID.randomUUID();
        System.clearProperty("fixture.rules.initialized.v12");
        try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host host =
            activityHost(
                client, packages, temp.resolve(queue), queue, rawHeader(entry.json()), attempts)) {
          host.start();
          io.temporal.client.WorkflowStub run =
              workflow(
                  client,
                  queue,
                  "activity-malformed-" + UUID.randomUUID(),
                  "HeaderActivityWorkflow");
          run.start(1);
          io.temporal.client.WorkflowFailedException error =
              assertThrows(
                  WorkflowFailedException.class,
                  () -> run.getResult(20, TimeUnit.SECONDS, String.class));
          ActivityFailure failure = assertInstanceOf(ActivityFailure.class, error.getCause());
          ApplicationFailure application =
              assertInstanceOf(ApplicationFailure.class, failure.getCause());
          assertTrue(
              application.getOriginalMessage(),
              application.getOriginalMessage().contains(entry.diagnostic()));
          assertFalse(application.isNonRetryable());
          assertEquals(Arrays.asList(1), attempts);
          assertNull(
              "a malformed header must not select current code",
              System.getProperty("fixture.rules.initialized.v12"));
          io.temporal.api.history.v1.ActivityTaskScheduledEventAttributes scheduled =
              client.fetchHistory(run.getExecution().getWorkflowId()).getEvents().stream()
                  .filter(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
                  .findFirst()
                  .orElseThrow(() -> new AssertionError("Expected matching history event"))
                  .getActivityTaskScheduledEventAttributes();
          assertEquals("ChargeCard", scheduled.getActivityType().getName());
          assertEquals(rawHeader(entry.json()).getValues(), scheduled.getHeader().getFieldsMap());
        }
      }
    }
  }

  @Test
  public void unavailableActivityPackageRemainsRetryableUntilConfiguredAttemptsAreExhausted()
      throws Exception {
    Path fixtures = temp.resolve("fixtures");
    Path original = FixtureCompiler.build(fixtures, 2);
    Path newer = FixtureCompiler.build(fixtures, 12);
    Path originalDirectory = Files.createDirectories(temp.resolve("original"));
    Files.copy(original, originalDirectory.resolve("orders.twp"));
    PackageRef originalRef = PackageCatalog.scan(originalDirectory).require("orders", 2).ref();
    Path packages = Files.createDirectories(temp.resolve("packages"));
    Files.copy(newer, packages.resolve("orders.twp"));
    Header reference = PackageHeaders.activity(Header.empty(), originalRef);
    CopyOnWriteArrayList<Integer> attempts = new CopyOnWriteArrayList<Integer>();
    String queue = "missing-activity-" + UUID.randomUUID();
    System.clearProperty("fixture.rules.initialized.v12");
    try (io.temporal.testing.TestWorkflowEnvironment env = TestServices.environment()) {
      env.start();
      io.temporal.client.WorkflowClient client =
          WorkflowClient.newInstance(env.getWorkflowServiceStubs());
      try (io.temporal.worker.packages.acceptance.IntegrationSupport.Host host =
          activityHost(client, packages, temp.resolve("cache"), queue, reference, attempts)) {
        host.start();
        io.temporal.client.WorkflowStub run =
            workflow(
                client, queue, "missing-activity-" + UUID.randomUUID(), "HeaderActivityWorkflow");
        run.start(3);
        io.temporal.client.WorkflowFailedException error =
            assertThrows(
                WorkflowFailedException.class,
                () -> run.getResult(20, TimeUnit.SECONDS, String.class));
        ActivityFailure failure = assertInstanceOf(ActivityFailure.class, error.getCause());
        ApplicationFailure application =
            assertInstanceOf(ApplicationFailure.class, failure.getCause());
        assertFalse(
            "missing package infrastructure failures remain retryable",
            application.isNonRetryable());
        assertTrue(
            application.getOriginalMessage(),
            application
                .getOriginalMessage()
                .contains("Missing activity type ChargeCard for version 2"));
        assertEquals(RetryState.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, failure.getRetryState());
        assertEquals(Arrays.asList(1, 2, 3), attempts);
        assertNull(
            "availability errors never fall back to current",
            System.getProperty("fixture.rules.initialized.v12"));
        io.temporal.api.history.v1.ActivityTaskScheduledEventAttributes scheduled =
            client.fetchHistory(run.getExecution().getWorkflowId()).getEvents().stream()
                .filter(e -> e.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected matching history event"))
                .getActivityTaskScheduledEventAttributes();
        assertEquals("ChargeCard", scheduled.getActivityType().getName());
        assertEquals(reference.getValues(), scheduled.getHeader().getFieldsMap());
      }
    }
  }

  private Host activityHost(
      WorkflowClient client,
      Path packages,
      Path cache,
      String queue,
      Header header,
      List<Integer> attempts)
      throws Exception {
    WorkerInterceptorBase injector =
        new WorkerInterceptorBase() {
          @Override
          public WorkflowInboundCallsInterceptor interceptWorkflow(
              WorkflowInboundCallsInterceptor next) {
            return new WorkflowInboundCallsInterceptorBase(next) {
              @Override
              public void init(WorkflowOutboundCallsInterceptor outbound) {
                super.init(
                    new WorkflowOutboundCallsInterceptorBase(outbound) {
                      @Override
                      public <R> ActivityOutput<R> executeActivity(ActivityInput<R> input) {
                        return super.executeActivity(
                            new ActivityInput<>(
                                input.getActivityName(),
                                input.getActivityId(),
                                input.getResultClass(),
                                input.getResultType(),
                                input.getArgs(),
                                input.getOptions(),
                                header));
                      }
                    });
              }
            };
          }
        };
    WorkerInterceptorBase counter =
        new WorkerInterceptorBase() {
          @Override
          public ActivityInboundCallsInterceptor interceptActivity(
              ActivityInboundCallsInterceptor next) {
            return new ActivityInboundCallsInterceptorBase(next) {
              private ActivityExecutionContext context;

              @Override
              public void init(ActivityExecutionContext value) {
                context = value;
                super.init(value);
              }

              @Override
              public ActivityOutput execute(ActivityInput input) {
                attempts.add(context.getInfo().getAttempt());
                return super.execute(input);
              }
            };
          }
        };
    WorkerPackagePlugin plugin =
        WorkerPackagePlugin.newBuilder()
            .setPackagesDirectory(packages)
            .setCacheDirectory(cache)
            .build();
    SimplePlugin last =
        new SimplePlugin("acceptance.activity-attempt-observer") {
          @Override
          public void configureWorkerFactory(WorkerFactoryOptions.Builder builder) {
            io.temporal.common.interceptors.WorkerInterceptor[] current =
                builder.build().getWorkerInterceptors();
            io.temporal.common.interceptors.WorkerInterceptor[] appended =
                Arrays.copyOf(current, current.length + 1);
            appended[current.length] = counter;
            builder.setWorkerInterceptors(appended);
          }
        };
    WorkerFactory factory = null;
    try {
      factory =
          WorkerFactory.newInstance(
              client,
              WorkerFactoryOptions.newBuilder()
                  .setWorkerInterceptors(injector)
                  .setPlugins(plugin, last)
                  .build());
      io.temporal.worker.Worker worker = factory.newWorker(queue);
      worker.registerWorkflowImplementationTypes(HeaderActivityWorkflowImpl.class);
      return new Host(plugin, factory, worker);
    } catch (Throwable error) {
      if (factory != null) {
        factory.shutdownNow();
        factory.awaitTermination(10, TimeUnit.SECONDS);
        if (factory.isTerminated()) plugin.closeAfterTermination(factory);
      } else plugin.close();
      throw error;
    }
  }

  private static Header rawHeader(String json) {
    if (json == null) return Header.empty();
    Payload payload =
        Payload.newBuilder()
            .putMetadata("encoding", ByteString.copyFromUtf8("json/plain"))
            .setData(ByteString.copyFromUtf8(json))
            .build();
    return new Header(TestSupport.map(PackageHeaders.KEY, payload));
  }

  private static final class InvalidHeader {
    private final String json;
    private final String diagnostic;

    InvalidHeader(String json, String diagnostic) {
      this.json = json;
      this.diagnostic = diagnostic;
    }

    String json() {
      return json;
    }

    String diagnostic() {
      return diagnostic;
    }
  }

  @WorkflowInterface
  public interface HeaderActivityWorkflow {
    @WorkflowMethod(name = "HeaderActivityWorkflow")
    String schedule(int maximumAttempts);
  }

  public static class HeaderActivityWorkflowImpl implements HeaderActivityWorkflow {
    public HeaderActivityWorkflowImpl() {}

    public String schedule(int maximumAttempts) {
      return Workflow.newUntypedActivityStub(
              ActivityOptions.newBuilder()
                  .setStartToCloseTimeout(Duration.ofSeconds(5))
                  .setRetryOptions(
                      RetryOptions.newBuilder()
                          .setInitialInterval(Duration.ofMillis(100))
                          .setMaximumInterval(Duration.ofMillis(100))
                          .setMaximumAttempts(maximumAttempts)
                          .build())
                  .build())
          .execute("ChargeCard", String.class, input("Never invoked"), 1);
    }
  }
}
