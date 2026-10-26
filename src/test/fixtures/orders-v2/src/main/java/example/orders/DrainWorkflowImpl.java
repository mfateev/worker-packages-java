package example.orders;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

public class DrainWorkflowImpl implements DrainWorkflow {
  public DrainWorkflowImpl() {}

  public String drain(String releasePath) {
    return Workflow.newActivityStub(
            PaymentActivities.class,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(60))
                .setHeartbeatTimeout(Duration.ofSeconds(5))
                .build())
        .drain(releasePath);
  }
}
