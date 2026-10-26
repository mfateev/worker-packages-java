package example.orders;

import io.temporal.workflow.*;

@WorkflowInterface
public interface OrderWorkflow {
  @WorkflowMethod
  String run(OrderInput input, boolean wait, boolean local);

  @SignalMethod
  void proceed(OrderInput input);

  @QueryMethod
  String state();

  @QueryMethod
  OrderInput details();

  @QueryMethod
  String badQuery();

  @UpdateMethod
  String rename(OrderInput input);

  @UpdateMethod
  String chargeNow(OrderInput input);

  @UpdateMethod
  OrderInput renameDetails(OrderInput input);

  @UpdateValidatorMethod(updateName = "rename")
  void validateRename(OrderInput input);
}
