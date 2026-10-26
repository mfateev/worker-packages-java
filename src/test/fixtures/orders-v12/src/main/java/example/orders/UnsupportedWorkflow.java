package example.orders;

import io.temporal.workflow.*;

@WorkflowInterface
public interface UnsupportedWorkflow {
  @WorkflowMethod
  String unsupported(String operation);
}
