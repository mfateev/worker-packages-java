package example.greeting;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

public class GreetingWorkflowImpl implements GreetingWorkflow {

  private final GreetingActivities activities =
      Workflow.newActivityStub(
          GreetingActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());

  private boolean released;

  @Override
  public String run() {
    // Pause so the demo can replace the worker before calling the activities.
    Workflow.await(() -> released);

    return activities.greet("World");
  }

  @Override
  public void proceed() {
    released = true;
  }
}
