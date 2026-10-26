package example.orders;

import io.temporal.activity.*;
import java.util.List;

@ActivityInterface
public interface PaymentActivities {
  @ActivityMethod(name = "ChargeCard")
  String charge(OrderInput input, int count);

  @ActivityMethod(name = "EchoOrders")
  List<OrderInput> echo(List<OrderInput> inputs);

  @ActivityMethod(name = "Notify")
  void notify(OrderInput input);

  @ActivityMethod(name = "WaitForCancellation")
  String awaitCancellation();

  @ActivityMethod(name = "InstanceNumber")
  int instanceNumber();

  @ActivityMethod(name = "Drain")
  String drain(String releasePath);
}
