package io.temporal.worker.packages.runtime;

import io.temporal.common.interceptors.WorkflowClientCallsInterceptor;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptorBase;
import io.temporal.common.interceptors.WorkflowClientInterceptorBase;

/** Opt-in client adapter; use a separate instance/client for each package ID. */
public final class PackageClientInterceptor extends WorkflowClientInterceptorBase {
  private final String packageId;

  public PackageClientInterceptor(String packageId) {
    PackageHeaders.validId(packageId);
    this.packageId = packageId;
  }

  @Override
  public WorkflowClientCallsInterceptor workflowClientCallsInterceptor(
      WorkflowClientCallsInterceptor next) {
    return new WorkflowClientCallsInterceptorBase(next) {
      private WorkflowStartInput routed(WorkflowStartInput input) {
        if (input.getOptions().getRetryOptions() != null
            || (input.getOptions().getCronSchedule() != null
                && !input.getOptions().getCronSchedule().isEmpty())) {
          throw new PackageRuntimeException(
              "Workflow retry policies and cron starts are unsupported");
        }
        return new WorkflowStartInput(
            input.getWorkflowId(),
            input.getWorkflowType(),
            PackageHeaders.workflow(input.getHeader(), packageId),
            input.getArguments(),
            input.getOptions());
      }

      @Override
      public WorkflowStartOutput start(WorkflowStartInput input) {
        return super.start(routed(input));
      }

      @Override
      public WorkflowSignalWithStartOutput signalWithStart(WorkflowSignalWithStartInput input) {
        return super.signalWithStart(
            new WorkflowSignalWithStartInput(
                routed(input.getWorkflowStartInput()),
                input.getSignalName(),
                input.getSignalArguments()));
      }

      @Override
      public <R> WorkflowUpdateWithStartOutput<R> updateWithStart(
          WorkflowUpdateWithStartInput<R> input) {
        throw new PackageRuntimeException("Update-With-Start is unsupported");
      }
    };
  }
}
