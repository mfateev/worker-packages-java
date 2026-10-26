package example.orders;

import io.temporal.workflow.*;

@WorkflowInterface
public interface DrainWorkflow {
  @WorkflowMethod
  String drain(String releasePath);
}
