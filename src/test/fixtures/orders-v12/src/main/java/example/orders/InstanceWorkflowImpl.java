package example.orders;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.*;
import java.time.Duration;
import java.util.*;

public class InstanceWorkflowImpl implements InstanceWorkflow {
  public InstanceWorkflowImpl() {}

  public List<Integer> instances(int count) {
    PaymentActivities activities =
        Workflow.newActivityStub(
            PaymentActivities.class,
            ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());
    List<Promise<Integer>> pending = new ArrayList<>();
    for (int i = 0; i < count; i++) pending.add(Async.function(activities::instanceNumber));
    List<Integer> result = new ArrayList<>();
    for (Promise<Integer> call : pending) result.add(call.get());
    return result;
  }
}
