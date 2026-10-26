package io.temporal.worker.packages.acceptance;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.api.common.v1.Payload;
import io.temporal.common.interceptors.*;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Installs caller metadata before the plugin sees an outbound activity request. */
final class ActivityContractInterceptor extends WorkerInterceptorBase {
  final AtomicInteger invocations = new AtomicInteger();
  final java.util.List<String> packageVersions = new java.util.concurrent.CopyOnWriteArrayList<>();
  final Payload tracing =
      Payload.newBuilder()
          .putMetadata("encoding", ByteString.copyFromUtf8("binary/plain"))
          .setData(ByteString.copyFromUtf8("opaque-trace-context"))
          .build();

  @Override
  public WorkflowInboundCallsInterceptor interceptWorkflow(WorkflowInboundCallsInterceptor next) {
    return new WorkflowInboundCallsInterceptorBase(next) {
      @Override
      public void init(WorkflowOutboundCallsInterceptor outbound) {
        super.init(
            new WorkflowOutboundCallsInterceptorBase(outbound) {
              private Header headers(Header original) {
                HashMap<String, Payload> fields = new HashMap<>(original.getValues());
                fields.put("traceparent", tracing);
                return new Header(fields);
              }

              @Override
              public <R> ActivityOutput<R> executeActivity(ActivityInput<R> input) {
                return super.executeActivity(
                    new ActivityInput<>(
                        input.getActivityName(),
                        "caller-" + input.getActivityName(),
                        input.getResultClass(),
                        input.getResultType(),
                        input.getArgs(),
                        input.getOptions(),
                        headers(input.getHeader())));
              }

              @Override
              public <R> LocalActivityOutput<R> executeLocalActivity(LocalActivityInput<R> input) {
                return super.executeLocalActivity(
                    new LocalActivityInput<>(
                        input.getActivityName(),
                        "caller-" + input.getActivityName(),
                        input.getResultClass(),
                        input.getResultType(),
                        input.getArgs(),
                        input.getOptions(),
                        headers(input.getHeader())));
              }
            });
      }
    };
  }

  @Override
  public ActivityInboundCallsInterceptor interceptActivity(ActivityInboundCallsInterceptor next) {
    return new ActivityInboundCallsInterceptorBase(next) {
      ActivityExecutionContext context;

      @Override
      public void init(ActivityExecutionContext context) {
        this.context = context;
        super.init(context);
      }

      @Override
      public ActivityOutput execute(ActivityInput input) {
        assertEquals("caller-ChargeCard", context.getInfo().getActivityId());
        assertEquals("ChargeCard", context.getInfo().getActivityType());
        assertEquals(tracing, input.getHeader().getValues().get("traceparent"));
        assertTrue(input.getHeader().getValues().containsKey(FixtureCompiler.HEADER));
        packageVersions.add(
            io.temporal.worker.packages.runtime.PackageHeaders.activityRef(input.getHeader())
                .packageVersion());
        invocations.incrementAndGet();
        return super.execute(input);
      }
    };
  }
}
