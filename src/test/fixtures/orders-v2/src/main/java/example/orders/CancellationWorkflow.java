package example.orders;

import io.temporal.workflow.*;

@WorkflowInterface
public interface CancellationWorkflow {
  @WorkflowMethod
  String cancelMe();
}
