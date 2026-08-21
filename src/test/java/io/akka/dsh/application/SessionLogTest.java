package io.akka.dsh.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.dsh.domain.PayloadRefused;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-001 L1 through L5, L7 and O2 — the log on its own, with no store around it. */
class SessionLogTest {

  private static SessionLog log() {
    return SessionLog.create("s1");
  }

  /** L1 — the log assigns the number, and it is the length. */
  @Test
  void seqIsTheLogLengthAndTheLogIsContiguousFromZero() {
    var s = log();
    assertEquals(0, s.seq());
    var a = s.append("turn/start", Map.of("turn", 1L));
    var b = s.append("turn/end", Map.of("turn", 1L));
    assertEquals(List.of(0, 1, 2), List.of(a.seq(), b.seq(), s.seq()));
    assertEquals(List.of(0, 1), s.events().stream().map(e -> e.seq()).toList());
  }

  /** L2 — writing to the object afterwards does not reach the logged event. */
  @Test
  void theLoggedPayloadIsASnapshotOfWhatTheCallerPassed() {
    var s = log();
    var data = new HashMap<String, Object>();
    data.put("turn", 1L);
    var logged = s.append("turn/start", data);
    data.put("turn", 999L);
    assertEquals(Map.of("turn", 1L), logged.data());
    assertEquals(Map.of("turn", 1L), s.events().get(0).data());
  }

  /** L3 — an accepted event refuses a write. */
  @Test
  void anAcceptedEventCannotBeWrittenThrough() {
    var logged = log().append("turn/start", Map.of("turn", 1L));
    @SuppressWarnings("unchecked")
    var data = (Map<String, Object>) logged.data();
    assertThrows(UnsupportedOperationException.class, () -> data.put("turn", 2L));
  }

  /** L4 — the refusal happens at the append call and the log does not move. */
  @Test
  void aRefusedAppendLeavesTheLogExactlyAsItWas() {
    var s = log();
    s.append("turn/start", Map.of("turn", 1L));
    var before = s.events();
    for (var bad : List.<Object>of(Double.NaN, -0.0d, new java.util.Date(0), java.util.Set.of("x"))) {
      assertThrows(PayloadRefused.class, () -> s.append("turn/start", Map.of("value", bad)));
    }
    assertEquals(1, s.seq());
    assertEquals(before, s.events());
  }

  /** L5 — a list handed out earlier keeps its length. */
  @Test
  void aViewHandedOutBeforeAnAppendDoesNotGrow() {
    var s = log();
    s.append("turn/start", Map.of("turn", 1L));
    var before = s.events();
    s.append("turn/end", Map.of("turn", 1L));
    assertEquals(1, before.size());
    assertEquals(2, s.events().size());
  }

  /** L7 — a log with no store around it accepts appends and tells nobody. */
  @Test
  void aSessionNoStoreHoldsStillAcceptsAppendsAndNotifiesNobody() {
    var s = log();
    s.append("turn/start", Map.of("turn", 1L));
    assertEquals(1, s.seq());
    assertTrue(s.observerCount() == 0);
  }

  /**
   * O2 — the bound is a rebuild decision the source does not have, and the point of it is
   * that exceeding it is something the caller is told about rather than something that
   * quietly does not happen.
   */
  @Test
  void appendPastTheBoundIsRefusedWithAVisibleCode() {
    var s = SessionLog.createBounded("bounded", 3);
    s.append("turn/start", Map.of("turn", 1L));
    s.append("turn/end", Map.of("turn", 1L));
    s.append("turn/start", Map.of("turn", 2L));

    var refused = assertThrows(PayloadRefused.class, () -> s.append("turn/end", Map.of("turn", 2L)));
    assertEquals("LOG_FULL", refused.code());
    assertEquals(3, s.seq());
  }

  /** The event's type and time come from the log, not from the payload. */
  @Test
  void eachEventCarriesItsTypeAndTheTimeItWasAccepted() {
    var before = System.currentTimeMillis();
    var logged = log().append("turn/start", Map.of("turn", 1L));
    assertEquals("turn/start", logged.type());
    assertTrue(logged.time() >= before);
  }

  /** An empty payload is a payload. */
  @Test
  void anEmptyPayloadIsAccepted() {
    assertEquals(Map.of(), log().append("session/end-seed", Map.of()).data());
  }
}
