package example.orders;

import io.temporal.activity.*;
import io.temporal.workflow.*;
import java.time.Duration;
import java.util.List;

public class PayloadWorkflowImpl implements PayloadWorkflow {
  public PayloadWorkflowImpl() {}

  public List<OrderInput> payloads(List<OrderInput> inputs, boolean local) {
    PaymentActivities activities =
        local
            ? Workflow.newLocalActivityStub(
                PaymentActivities.class,
                LocalActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    .build())
            : Workflow.newActivityStub(
                PaymentActivities.class,
                ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    .build());
    activities.notify(inputs.get(0));
    return activities.echo(inputs);
  }
}
