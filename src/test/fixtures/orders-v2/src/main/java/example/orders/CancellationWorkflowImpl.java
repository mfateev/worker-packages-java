package example.orders;

import io.temporal.activity.*;
import io.temporal.workflow.Workflow;
import java.time.Duration;

public class CancellationWorkflowImpl implements CancellationWorkflow {
  public CancellationWorkflowImpl() {}

  public String cancelMe() {
    return Workflow.newActivityStub(
            PaymentActivities.class,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(40))
                .setHeartbeatTimeout(Duration.ofSeconds(2))
                .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
                .build())
        .awaitCancellation();
  }
}
