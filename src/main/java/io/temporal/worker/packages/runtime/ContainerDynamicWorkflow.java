package io.temporal.worker.packages.runtime;

import io.temporal.common.converter.EncodedValues;
import io.temporal.worker.packages.api.PackageRef;
import io.temporal.worker.packages.tooling.ValidatedPackage;
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
              String version = PackageRef.validateVersion(versionSelector.selectVersion(context));
              PackageRef reference =
                  runtime.catalog().referenceForWorkflow(state.packageId, version);
              ValidatedPackage selected = runtime.catalog().resolveForExecution(reference);
              if (selected.manifest().workflows().stream()
                  .noneMatch(entry -> logicalType.equals(entry.type())))
                throw new PackageRuntimeException(
                    "Package "
                        + state.packageId
                        + "/"
                        + version
                        + " does not support workflow type "
                        + logicalType);
              return new String[] {reference.packageVersion(), reference.sha256()};
            });
    if (pin == null || pin.length != 2)
      throw new PackageRuntimeException("Invalid durable worker package pin");
    PackageRef ref = new PackageRef(state.packageId, pin[0], pin[1]);
    runtime.catalog().resolveForExecution(ref);
    state.packageRef = ref;
    return runtime.invokeWorkflow(ref, logicalType, args, state);
  }
}
