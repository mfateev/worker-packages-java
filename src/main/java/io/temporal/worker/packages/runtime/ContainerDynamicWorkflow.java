package io.temporal.worker.packages.runtime;

import io.temporal.common.converter.EncodedValues;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.workflow.DynamicWorkflow;
import io.temporal.workflow.Workflow;

/** Public no-argument host dispatcher; binding happens through the public inbound interceptor. */
public final class ContainerDynamicWorkflow implements DynamicWorkflow {
  private PreparedPackageRuntime runtime;
  private RunDispatchState state;
  private WorkerPackagePlugin.VersionSelector versionSelector;

  public ContainerDynamicWorkflow() {}

  void bind(
      PreparedPackageRuntime runtime,
      RunDispatchState state,
      WorkerPackagePlugin.VersionSelector versionSelector) {
    if (this.runtime != null)
      throw new PackageRuntimeException("Workflow dispatcher already bound");
    this.runtime = runtime;
    this.state = state;
    this.versionSelector = versionSelector;
  }

  @Override
  public Object execute(EncodedValues args) {
    if (runtime == null || state == null)
      throw new PackageRuntimeException("Worker package plugin is not configured");
    String logicalType = Workflow.getInfo().getWorkflowType();
    String[] pin =
        Workflow.sideEffect(
            String[].class,
            () -> {
              WorkerPackagePlugin.SelectionContext context =
                  new WorkerPackagePlugin.SelectionContext(
                      state.packageId, Workflow.getInfo(), runtime.catalog());
              String version =
                  PackageRef.validateVersion(
                      state.requestedVersion == null
                          ? versionSelector.selectVersion(context)
                          : state.requestedVersion);
              PackageRef reference =
                  runtime.catalog().referenceForWorkflowType(logicalType, version);
              return new String[] {
                reference.packageVersion(), reference.sha256(), reference.packageId()
              };
            });
    if (pin == null || (pin.length != 2 && pin.length != 3))
      throw new PackageRuntimeException("Invalid durable worker package pin");
    PackageRef ref = new PackageRef(pin.length == 3 ? pin[2] : state.packageId, pin[0], pin[1]);
    runtime.catalog().resolveForExecution(ref, logicalType, true);
    state.packageRef = ref;
    return runtime.invokeWorkflow(ref, logicalType, args, state);
  }
}
