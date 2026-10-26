package example.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public class JacksonWorkflowImpl implements JacksonWorkflow {
  public JacksonWorkflowImpl() {}

  public Map<String, String> privateJackson(OrderInput input) {
    ObjectMapper mapper = new ObjectMapper();
    if (mapper.getClass().getClassLoader() != getClass().getClassLoader())
      throw new IllegalStateException("databind not private");
    if (mapper.getFactory().getClass().getClassLoader() != getClass().getClassLoader())
      throw new IllegalStateException("core not private");
    try {
      Map<String, String> result = new java.util.LinkedHashMap<>();
      result.put("databind", mapper.version().toString());
      result.put("core", mapper.getFactory().version().toString());
      result.put("wire", mapper.writeValueAsString(input));
      return result;
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
