package example.greeting;

import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface GreetingActivities {
  String getName();

  String greet(String greeting, String name);
}
