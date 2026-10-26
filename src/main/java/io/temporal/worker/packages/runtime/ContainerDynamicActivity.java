package io.temporal.worker.packages.runtime;

import io.temporal.activity.Activity;
import io.temporal.activity.DynamicActivity;
import io.temporal.common.converter.EncodedValues;
import io.temporal.worker.packages.api.PackageRef;
import java.util.Objects;

/** Stateless shared adapter. Routing state travels in a per-attempt in-memory carrier. */
final class ContainerDynamicActivity implements DynamicActivity {
  private final PreparedPackageRuntime runtime;

  ContainerDynamicActivity(PreparedPackageRuntime runtime) {
    this.runtime = runtime;
  }

  static final class Invocation {
    private final PackageRef ref;
    private final EncodedValues originalArguments;

    Invocation(PackageRef ref, EncodedValues originalArguments) {
      this.ref = ref;
      this.originalArguments = originalArguments;
    }

    public PackageRef ref() {
      return ref;
    }

    public EncodedValues originalArguments() {
      return originalArguments;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (!(other instanceof Invocation)) return false;
      Invocation that = (Invocation) other;
      return Objects.equals(ref, that.ref)
          && Objects.equals(originalArguments, that.originalArguments);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + Objects.hashCode(ref);
      result = 31 * result + Objects.hashCode(originalArguments);
      return result;
    }

    @Override
    public String toString() {
      return "Invocation[ref=" + ref + ", originalArguments=" + originalArguments + "]";
    }
  }

  @Override
  public Object execute(EncodedValues input) {
    if (input.getSize() != 1)
      throw new PackageRuntimeException("Activity package header bridge is not configured");
    Invocation invocation;
    try {
      invocation = input.get(0, Invocation.class);
    } catch (RuntimeException e) {
      throw new PackageRuntimeException("Activity package header bridge is not configured", e);
    }
    return runtime.invokeActivity(
        invocation.ref(),
        Activity.getExecutionContext().getInfo().getActivityType(),
        invocation.originalArguments());
  }
}
