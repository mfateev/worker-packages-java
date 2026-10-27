package io.temporal.worker.packages.runtime;

import io.temporal.activity.ActivityExecutionContext;
import io.temporal.common.converter.EncodedValues;
import io.temporal.common.interceptors.ActivityInboundCallsInterceptor;
import io.temporal.common.interceptors.ActivityInboundCallsInterceptorBase;
import io.temporal.common.interceptors.WorkerInterceptorBase;
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptorBase;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptorBase;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.workflow.Workflow;

final class PackageWorkerInterceptor extends WorkerInterceptorBase {
  private final PreparedPackageRuntime runtime;
  private final WorkerPackagePlugin.VersionSelector versionSelector;

  PackageWorkerInterceptor(
      PreparedPackageRuntime runtime, WorkerPackagePlugin.VersionSelector versionSelector) {
    this.runtime = runtime;
    this.versionSelector = versionSelector;
  }

  @Override
  public WorkflowInboundCallsInterceptor interceptWorkflow(WorkflowInboundCallsInterceptor next) {
    RunDispatchState state = new RunDispatchState();
    return new WorkflowInboundCallsInterceptorBase(next) {
      @Override
      public void init(WorkflowOutboundCallsInterceptor outboundCalls) {
        super.init(
            new WorkflowOutboundCallsInterceptorBase(outboundCalls) {
              @Override
              public <R> ActivityOutput<R> executeActivity(ActivityInput<R> input) {
                if (!state.managed) return super.executeActivity(input);
                PackageRef activityRef = activityRef(state, input.getActivityName());
                return super.executeActivity(
                    new ActivityInput<>(
                        input.getActivityName(),
                        input.getActivityId(),
                        input.getResultClass(),
                        input.getResultType(),
                        input.getArgs(),
                        input.getOptions(),
                        PackageHeaders.activity(input.getHeader(), activityRef)));
              }

              @Override
              public <R> LocalActivityOutput<R> executeLocalActivity(LocalActivityInput<R> input) {
                if (!state.managed) return super.executeLocalActivity(input);
                PackageRef activityRef = activityRef(state, input.getActivityName());
                return super.executeLocalActivity(
                    new LocalActivityInput<>(
                        input.getActivityName(),
                        input.getActivityId(),
                        input.getResultClass(),
                        input.getResultType(),
                        input.getArgs(),
                        input.getOptions(),
                        PackageHeaders.activity(input.getHeader(), activityRef)));
              }

              @Override
              public <R> ChildWorkflowOutput<R> executeChildWorkflow(ChildWorkflowInput<R> input) {
                if (state.managed)
                  throw new PackageRuntimeException(
                      "Child workflows are unsupported for packaged workflows");
                return super.executeChildWorkflow(input);
              }

              @Override
              public void continueAsNew(ContinueAsNewInput input) {
                if (state.managed)
                  throw new PackageRuntimeException(
                      "Continue-As-New is unsupported for packaged workflows");
                super.continueAsNew(input);
              }

              @Override
              public Object newChildThread(Runnable runnable, boolean detached, String name) {
                return super.newChildThread(scoped(state, runnable), detached, name);
              }
            });
      }

      @Override
      public WorkflowOutput execute(WorkflowInput input) {
        Object instance = Workflow.getInstance();
        if (!(instance instanceof ContainerDynamicWorkflow)) return super.execute(input);
        ContainerDynamicWorkflow dispatcher = (ContainerDynamicWorkflow) instance;
        runtime.checkOpen();
        if (Workflow.getInfo().getRetryOptions() != null
            || (Workflow.getInfo().getCronSchedule() != null
                && !Workflow.getInfo().getCronSchedule().isEmpty())) {
          throw new PackageRuntimeException(
              "Workflow retry policies and cron runs are unsupported");
        }
        state.managed = true;
        state.packageId = PackageHeaders.workflowPackageId(input.getHeader());
        state.requestedVersion = PackageHeaders.workflowVersion(input.getHeader());
        dispatcher.bind(runtime, state, versionSelector);
        return super.execute(input);
      }

      @Override
      public void handleSignal(SignalInput input) {
        PreparedPackageRuntime.withLoader(
            state.loader,
            () -> {
              super.handleSignal(input);
              return null;
            });
      }

      @Override
      public QueryOutput handleQuery(QueryInput input) {
        return PreparedPackageRuntime.withLoader(state.loader, () -> super.handleQuery(input));
      }

      @Override
      public void validateUpdate(UpdateInput input) {
        PreparedPackageRuntime.withLoader(
            state.loader,
            () -> {
              super.validateUpdate(input);
              return null;
            });
      }

      @Override
      public UpdateOutput executeUpdate(UpdateInput input) {
        return PreparedPackageRuntime.withLoader(state.loader, () -> super.executeUpdate(input));
      }

      @Override
      public Object newWorkflowMethodThread(Runnable runnable, String name) {
        return super.newWorkflowMethodThread(scoped(state, runnable), name);
      }

      @Override
      public Object newCallbackThread(Runnable runnable, String name) {
        return super.newCallbackThread(scoped(state, runnable), name);
      }
    };
  }

  private PackageRef activityRef(RunDispatchState state, String type) {
    if (state.packageRef == null)
      throw new PackageRuntimeException("Activity scheduled before package selection");
    PackageRef activity =
        runtime.catalog().referenceForActivityType(type, state.packageRef.packageVersion());
    // Preserve the recorded workflow reference when an explicit patch replaces that same archive.
    if (activity.packageId().equals(state.packageRef.packageId())
        && !runtime
            .catalog()
            .resolveForExecution(activity, type, false)
            .ref()
            .packageVersion()
            .equals(activity.packageVersion())) return state.packageRef;
    return activity;
  }

  private static Runnable scoped(RunDispatchState state, Runnable runnable) {
    return () ->
        PreparedPackageRuntime.withLoader(
            state.loader,
            () -> {
              runnable.run();
              return null;
            });
  }

  @Override
  public ActivityInboundCallsInterceptor interceptActivity(ActivityInboundCallsInterceptor next) {
    return new ActivityInboundCallsInterceptorBase(next) {
      private boolean managed;
      private ActivityExecutionContext context;

      @Override
      public void init(ActivityExecutionContext context) {
        this.context = context;
        managed = context.getInstance() instanceof ContainerDynamicActivity;
        super.init(context);
      }

      @Override
      public ActivityOutput execute(ActivityInput input) {
        if (!managed) return super.execute(input);
        PackageRef ref = PackageHeaders.activityRef(input.getHeader());
        runtime.catalog().resolveForExecution(ref, context.getInfo().getActivityType(), false);
        if (input.getArguments().length != 1
            || !(input.getArguments()[0] instanceof EncodedValues)) {
          throw new PackageRuntimeException(
              "Unexpected SDK dynamic activity argument representation");
        }
        EncodedValues original = (EncodedValues) input.getArguments()[0];
        // This adapter exists only in this attempt's interceptor chain. No wire inputs are
        // rewritten.
        EncodedValues bridged =
            new EncodedValues(new ContainerDynamicActivity.Invocation(ref, original));
        return super.execute(new ActivityInput(input.getHeader(), new Object[] {bridged}));
      }
    };
  }
}
