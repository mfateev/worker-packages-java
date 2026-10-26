package example.orders;

import io.temporal.workflow.*;

public class UnsupportedWorkflowImpl implements UnsupportedWorkflow {
  public UnsupportedWorkflowImpl() {}

  public String unsupported(String operation) {
    try {
      if (operation.equals("child"))
        Workflow.newUntypedChildWorkflowStub("OrderWorkflow")
            .execute(String.class, new OrderInput("child"), false, false);
      else Workflow.continueAsNew(operation);
      return "unexpectedly allowed";
    } catch (RuntimeException e) {
      return e.getMessage();
    }
  }
}
