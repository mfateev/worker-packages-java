package example.greeting;

public class GreetingActivitiesImpl implements GreetingActivities {
  @Override
  public String greet(String name) {
    return "Hello, " + name + "!";
  }
}
