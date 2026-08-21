package io.akka.dsh.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-001 L2, L4 and O3 — which payloads are accepted, and what an accepted one becomes. */
class PayloadValidatorTest {

  /**
   * Each of these is a value the source refuses at its append site. Three of them the
   * target's serializer would have accepted — a non-finite number silently as a string,
   * a negative zero as a negative zero, an exotic object as whatever its default mapping
   * is — so this rule is the rebuild's own, not the serializer's.
   */
  @Test
  void everyPayloadWithNoFaithfulJsonSpellingIsRefused() {
    var circular = new HashMap<String, Object>();
    circular.put("self", circular);

    var cases =
        new LinkedHashMap<String, Object>();
    cases.put("not a number", Double.NaN);
    cases.put("positive infinity", Double.POSITIVE_INFINITY);
    cases.put("negative infinity", Double.NEGATIVE_INFINITY);
    cases.put("negative zero", -0.0d);
    cases.put("a date", new Date(0));
    cases.put("a set", java.util.Set.of("a"));
    cases.put("a class instance", new Object());
    cases.put("a containing itself", circular);

    for (var entry : cases.entrySet()) {
      var refused =
          assertThrows(
              PayloadRefused.class,
              () -> PayloadValidator.snapshot(Map.of("value", entry.getValue())),
              entry.getKey() + " should be refused");
      assertEquals("NOT_JSON", refused.code());
    }
  }

  /** A nested non-JSON value is found however deep it sits. */
  @Test
  void aRefusedValueIsFoundInsideNestingRatherThanOnlyAtTheTop() {
    var deep = Map.of("a", List.of(Map.of("b", List.of(Double.NaN))));
    assertEquals("NOT_JSON", assertThrows(PayloadRefused.class, () -> PayloadValidator.snapshot(deep)).code());
  }

  /**
   * The snapshot is taken during validation, in the same pass. A caller that keeps a
   * reference to what it passed and writes to it afterwards must not be able to change
   * what was stored.
   */
  @Test
  void theSnapshotIsDetachedFromWhatTheCallerPassed() {
    var inner = new ArrayList<Object>(List.of(1L, 2L));
    var outer = new HashMap<String, Object>();
    outer.put("items", inner);

    var stored = PayloadValidator.snapshot(outer);
    inner.add(3L);
    outer.put("added", "later");

    assertEquals(Map.of("items", List.of(1L, 2L)), stored);
    assertNotSame(inner, ((Map<?, ?>) stored).get("items"));
  }

  /** SPEC-001 L3 — an accepted payload cannot be written through at any depth. */
  @Test
  void theSnapshotCannotBeWrittenThrough() {
    var stored = PayloadValidator.snapshot(Map.of("items", List.of(Map.of("k", "v"))));
    @SuppressWarnings("unchecked")
    var asMap = (Map<String, Object>) stored;
    assertThrows(UnsupportedOperationException.class, () -> asMap.put("k", "v"));
    @SuppressWarnings("unchecked")
    var items = (List<Object>) asMap.get("items");
    assertThrows(UnsupportedOperationException.class, () -> items.add("x"));
    @SuppressWarnings("unchecked")
    var first = (Map<String, Object>) items.get(0);
    assertThrows(UnsupportedOperationException.class, () -> first.put("k", "w"));
  }

  /** The values JSON does have a spelling for all survive a snapshot unchanged. */
  @Test
  void theValuesJsonCanSpellSurviveUnchanged() {
    var map = new HashMap<String, Object>();
    map.put("string", "s");
    map.put("long", 7L);
    map.put("double", 1.5d);
    map.put("bool", true);
    map.put("null", null);
    map.put("list", List.of("a", 1L));
    map.put("nested", Map.of("k", "v"));
    var stored = PayloadValidator.snapshot(map);
    assertEquals(map, stored);
    assertTrue(((Map<?, ?>) stored).containsKey("null"));
  }

  /** A map key that is not a string has no JSON spelling either. */
  @Test
  void aNonStringKeyIsRefused() {
    var map = new HashMap<Object, Object>();
    map.put(1L, "v");
    assertEquals("NOT_JSON", assertThrows(PayloadRefused.class, () -> PayloadValidator.snapshot(map)).code());
  }
}
