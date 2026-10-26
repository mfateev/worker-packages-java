package example.orders;

import io.temporal.workflow.*;
import java.util.Map;

@WorkflowInterface
public interface JacksonWorkflow {
  @WorkflowMethod
  Map<String, String> privateJackson(OrderInput input);
}
