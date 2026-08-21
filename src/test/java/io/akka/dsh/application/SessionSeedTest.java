package io.akka.dsh.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.dsh.domain.PayloadRefused;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-001 S1 and S2 — what construction from replayed history does. */
class SessionSeedTest {

  private static SessionEvent event(int seq, String type) {
    return new SessionEvent(seq, type, seq + 1L, Map.of());
  }

  /** S2 — one marker, and reopening the result does not add a second. */
  @Test
  void aSeedGainsOneEndOfSeedMarkerAndReopeningDoesNotAddASecond() {
    var first = SessionLog.seeded("seeded", List.of(event(0, "turn/start"), event(1, "turn/end")));
    assertEquals(
        List.of("turn/start", "turn/end", "session/end-seed"),
        first.events().stream().map(SessionEvent::type).toList());
    assertEquals(2, first.firstLiveSeq());

    var reopened = SessionLog.seeded("seeded-again", first.events());
    assertEquals(
        List.of("turn/start", "turn/end", "session/end-seed"),
        reopened.events().stream().map(SessionEvent::type).toList());
    assertEquals(3, reopened.firstLiveSeq());
  }

  /** S1 — the same contiguity rule a live append is held to. */
  @Test
  void aSeedIsHeldToTheSameRulesAsALiveAppend() {
    var gap = List.of(event(0, "turn/start"), event(2, "turn/end"));
    var refused = assertThrows(PayloadRefused.class, () -> SessionLog.seeded("gap", gap));
    assertEquals("SEED_NOT_CONTIGUOUS", refused.code());
    assertTrue(refused.getMessage().contains("index 1"));

    var startsLate = List.of(event(3, "turn/start"));
    assertEquals(
        "SEED_NOT_CONTIGUOUS",
        assertThrows(PayloadRefused.class, () -> SessionLog.seeded("late", startsLate)).code());
  }

  /** S1 — and the same payload rule. */
  @Test
  void aSeedCarryingAPayloadWithNoJsonSpellingIsRefused() {
    var bad = List.of(new SessionEvent(0, "turn/start", 1L, Map.of("value", Double.NaN)));
    assertEquals(
        "NOT_JSON", assertThrows(PayloadRefused.class, () -> SessionLog.seeded("bad", bad)).code());
  }

  /** An empty seed is still a seed, so it is still marked — which is what makes the
   * empty fork in S6 come out the way it does. */
  @Test
  void anEmptySeedIsStillMarked() {
    var s = SessionLog.seeded("empty-seed", List.of());
    assertEquals(List.of("session/end-seed"), s.events().stream().map(SessionEvent::type).toList());
    assertEquals(0, s.firstLiveSeq());
  }

  /** A session created without a seed at all has no marker and nothing to mark. */
  @Test
  void aSessionCreatedWithoutASeedHasNoMarker() {
    var s = SessionLog.create("unseeded");
    assertEquals(List.of(), s.events());
    assertEquals(0, s.firstLiveSeq());
  }

  /** Appending after a seed continues the same numbering. */
  @Test
  void aLiveAppendAfterASeedContinuesTheSameNumbering() {
    var s = SessionLog.seeded("continues", List.of(event(0, "turn/start")));
    assertEquals(2, s.append("turn/end", Map.of("turn", 1L)).seq());
  }
}
