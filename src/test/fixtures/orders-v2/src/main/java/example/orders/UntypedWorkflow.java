package example.orders;

import io.temporal.workflow.*;

@WorkflowInterface
public interface UntypedWorkflow {
  @WorkflowMethod
  String untyped(OrderInput input, boolean local, String activityType);
}
