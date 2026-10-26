package example.orders;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderInput {
  @JsonProperty("customer_name")
  public String customer;

  @JsonIgnore public String secret;

  public OrderInput() {}

  public OrderInput(String customer) {
    this.customer = customer;
    this.secret = "must-not-encode";
  }
}
