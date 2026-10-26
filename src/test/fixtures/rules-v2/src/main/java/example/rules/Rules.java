package example.rules;

public final class Rules {
  static {
    System.setProperty("fixture.rules.initialized.v2", "yes");
  }

  public static String describe(String name, int count) {
    return "rules-v2:" + name + ":" + count;
  }
}
