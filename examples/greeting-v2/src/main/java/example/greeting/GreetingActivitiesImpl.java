package example.greeting;

public class GreetingActivitiesImpl implements GreetingActivities {
  @Override
  public String greet(String greeting, String name) {
    return greeting + ", " + name + "!";
  }

  @Override
  public String getName() {
    return "multi-versioned worker";
  }
}
