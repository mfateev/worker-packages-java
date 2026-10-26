package example.orders;

import io.temporal.workflow.*;
import java.util.List;

@WorkflowInterface
public interface InstanceWorkflow {
  @WorkflowMethod
  List<Integer> instances(int count);
}
