package example.orders;

import io.temporal.activity.*;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.*;
import java.io.InputStream;
import java.time.Duration;

public class OrderWorkflowImpl implements OrderWorkflow {
  private String customer;
  private boolean released;

  public OrderWorkflowImpl() {
    System.setProperty("fixture.workflow.invoked.v2", "yes");
  }

  public String run(OrderInput input, boolean wait, boolean local) {
    if (customer == null) customer = input.customer;
    assertLoader();
    Workflow.getVersion("application.patch", Workflow.DEFAULT_VERSION, 3);
    Workflow.getMetricsScope().counter("fixture_workflow").inc(1);
    String resource =
        Async.function(
                () -> {
                  assertLoader();
                  try (InputStream in =
                      Thread.currentThread()
                          .getContextClassLoader()
                          .getResourceAsStream("rules-version.txt")) {
                    return ResourceContents.read(in).trim();
                  } catch (java.io.IOException e) {
                    throw new IllegalStateException(e);
                  }
                })
            .get();
    if (wait) Workflow.await(() -> released);
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
                    .setHeartbeatTimeout(Duration.ofSeconds(5))
                    .setRetryOptions(
                        RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(10))
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .setMaximumAttempts(3)
                            .build())
                    .build());
    return "workflow-v2|"
        + activities.charge(new OrderInput(customer), 3)
        + "|resource="
        + resource;
  }

  public void proceed(OrderInput input) {
    assertLoader();
    customer = input.customer;
    released = true;
  }

  public String state() {
    assertLoader();
    return "v2:" + customer;
  }

  public String rename(OrderInput input) {
    assertLoader();
    customer = input.customer;
    return state();
  }

  public String badQuery() {
    assertLoader();
    Workflow.sleep(Duration.ofMillis(1));
    return "should-not-succeed";
  }

  public OrderInput details() {
    assertLoader();
    return new OrderInput(customer);
  }

  public OrderInput renameDetails(OrderInput input) {
    assertLoader();
    customer = input.customer;
    return new OrderInput(customer);
  }

  public String chargeNow(OrderInput input) {
    assertLoader();
    return Workflow.newActivityStub(
            PaymentActivities.class,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(10))
                .setHeartbeatTimeout(Duration.ofSeconds(5))
                .setRetryOptions(
                    RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofSeconds(10))
                        .setMaximumInterval(Duration.ofSeconds(10))
                        .setMaximumAttempts(3)
                        .build())
                .build())
        .charge(input, 1);
  }

  public void validateRename(OrderInput input) {
    assertLoader();
    if (input.customer == null || input.customer.trim().isEmpty())
      throw new IllegalArgumentException("customer must not be empty");
  }

  private void assertLoader() {
    if (Thread.currentThread().getContextClassLoader() != getClass().getClassLoader())
      throw new IllegalStateException("wrong workflow TCCL");
  }
}
