package io.akka.dsh.domain;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-001 D3 — what stops a dispatch. */
class BailRuleTest {

  /**
   * The rule is about absence, not about truthiness: zero and the empty string stop a
   * dispatch. Getting this backwards would turn a listener returning a legitimate zero
   * into a listener that appears to have declined.
   */
  @Test
  void zeroAndEmptyStringStopADispatchWhileNullAndFalseDoNot() {
    assertTrue(BailRule.stops(0));
    assertTrue(BailRule.stops(0L));
    assertTrue(BailRule.stops(0.0d));
    assertTrue(BailRule.stops(""));
    assertTrue(BailRule.stops(Double.NaN));
    assertTrue(BailRule.stops(List.of()));
    assertTrue(BailRule.stops(Map.of()));
    assertTrue(BailRule.stops("something"));
    assertTrue(BailRule.stops(true));

    assertFalse(BailRule.stops(null));
    assertFalse(BailRule.stops(false));
    assertFalse(BailRule.stops(Boolean.FALSE));
  }
}
