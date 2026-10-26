package example.orders;

import io.temporal.activity.*;
import io.temporal.workflow.*;
import java.time.Duration;

public class UntypedWorkflowImpl implements UntypedWorkflow {
  public UntypedWorkflowImpl() {}

  public String untyped(OrderInput input, boolean local, String activityType) {
    try {
      if (local)
        return Workflow.newUntypedLocalActivityStub(
                LocalActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    .build())
            .execute(activityType, String.class, input, 3);
      return Workflow.newUntypedActivityStub(
              ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build())
          .execute(activityType, String.class, input, 3);
    } catch (RuntimeException e) {
      return e.getMessage();
    }
  }
}
