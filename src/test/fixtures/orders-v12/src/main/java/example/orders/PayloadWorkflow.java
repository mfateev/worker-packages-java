package example.orders;

import io.temporal.workflow.*;
import java.util.List;

@WorkflowInterface
public interface PayloadWorkflow {
  @WorkflowMethod
  List<OrderInput> payloads(List<OrderInput> inputs, boolean local);
}
