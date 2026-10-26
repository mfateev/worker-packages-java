package example.orders;

import example.rules.Rules;
import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import java.util.List;

public class PaymentActivitiesImpl implements PaymentActivities {
  private static final java.util.concurrent.atomic.AtomicInteger INSTANCES =
      new java.util.concurrent.atomic.AtomicInteger();
  private final int number;

  public PaymentActivitiesImpl() {
    number = INSTANCES.incrementAndGet();
  }

  public int instanceNumber() {
    return number;
  }

  public String charge(OrderInput input, int count) {
    if (input.customer.startsWith("fail:"))
      throw ApplicationFailure.newNonRetryableFailure("business rejection", "FixtureBusiness");
    if (input.customer.startsWith("retry:")
        && Activity.getExecutionContext().getInfo().getAttempt() == 1) {
      throw ApplicationFailure.newFailure(
          "first attempt is intentionally retryable", "FixtureRetry");
    }
    Activity.getExecutionContext().getMetricsScope().counter("fixture_activity").inc(1);
    if (!Activity.getExecutionContext().getInfo().isLocal())
      Activity.getExecutionContext().heartbeat("charging");
    if (Thread.currentThread().getContextClassLoader() != getClass().getClassLoader())
      throw new IllegalStateException("wrong activity TCCL");
    return Rules.describe(input.customer, count);
  }

  public String drain(String releasePath) {
    while (!java.nio.file.Files.exists(java.nio.file.Paths.get(releasePath))) {
      Activity.getExecutionContext().heartbeat("waiting-for-release");
      try {
        Thread.sleep(25);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Activity.wrap(e);
      }
    }
    try (java.io.InputStream in =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("rules-version.txt")) {
      if (in == null)
        throw new IllegalStateException("package resources closed before activity drained");
      return "drained-" + ResourceContents.read(in).trim();
    } catch (java.io.IOException e) {
      throw Activity.wrap(e);
    }
  }

  public String awaitCancellation() {
    while (true) {
      Activity.getExecutionContext().heartbeat("waiting-for-cancellation");
      try {
        Thread.sleep(25);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Activity.wrap(e);
      }
    }
  }

  public List<OrderInput> echo(List<OrderInput> inputs) {
    return inputs;
  }

  public void notify(OrderInput input) {
    if (input.customer == null) throw new IllegalArgumentException("customer required");
  }
}
