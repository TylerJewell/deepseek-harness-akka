package io.akka.dsh.domain;

/**
 * What stops a dispatch. The rule is about absence rather than about truth: a listener
 * returning zero or an empty string has answered, and the dispatch stops there.
 */
public final class BailRule {

  private BailRule() {}

  /** @return whether {@code value} stops a dispatch. */
  public static boolean stops(Object value) {
    return value != null && !Boolean.FALSE.equals(value);
  }
}
