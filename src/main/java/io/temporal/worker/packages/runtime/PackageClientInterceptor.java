package io.temporal.worker.packages.runtime;

import io.temporal.common.interceptors.WorkflowClientCallsInterceptor;
import io.temporal.common.interceptors.WorkflowClientCallsInterceptorBase;
import io.temporal.common.interceptors.WorkflowClientInterceptorBase;

/** Client adapter for worker-assigned or explicit workflow versions. No archives are required. */
public final class PackageClientInterceptor extends WorkflowClientInterceptorBase {
  private final String version;

  public PackageClientInterceptor() {
    this.version = null;
  }

  public PackageClientInterceptor(String version) {
    this.version = io.temporal.worker.packages.api.PackageRef.validateVersion(version);
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
            version == null
                ? input.getHeader()
                : PackageHeaders.workflow(input.getHeader(), version),
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
