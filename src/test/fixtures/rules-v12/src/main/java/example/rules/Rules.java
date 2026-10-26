package example.rules;

public final class Rules {
  static {
    System.setProperty("fixture.rules.initialized.v12", "yes");
  }

  public String describe(int count, String name) {
    return "rules-v12:" + name + ":" + count;
  }
}
