package io.akka.dsh.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** SPEC-001 S3 through S6 — forking a stable prefix. */
class SessionForkTest {

  private final SessionStore store = new SessionStore(new EventBus());

  private SessionLog parent(String id) {
    var s = store.create(id);
    s.append("turn/start", Map.of("turn", 1L));
    s.append("turn/end", Map.of("turn", 1L));
    s.append("turn/start", Map.of("turn", 2L));
    return s;
  }

  /** S3 — boundary 1 gives events 0 and 1, and the child says where it came from. */
  @Test
  void aBoundaryIsInclusiveAndTheChildRecordsItsLineage() {
    var child = store.fork(parent("parent"), Optional.of(1), Optional.of("child"));
    assertEquals(
        List.of("turn/start", "turn/end", "session/end-seed"),
        child.events().stream().map(SessionEvent::type).toList());
    assertEquals(Optional.of("parent"), child.parentSession());
    assertEquals(2, child.seedLength());
  }

  /** An omitted boundary forks through the source's last event. */
  @Test
  void anOmittedBoundaryForksThroughTheLastEvent() {
    var source = store.create("whole");
    source.append("turn/start", Map.of("turn", 1L));
    source.append("turn/end", Map.of("turn", 1L));
    var child = store.fork(source, Optional.empty(), Optional.empty());
    assertEquals(3, child.events().size());
    assertEquals(2, child.seedLength());
  }

  /** S4 — a prefix whose last turn marker is a start is refused. */
  @Test
  void aPrefixEndingInsideAnOpenTurnIsRefused() {
    var refused =
        assertThrows(
            SessionForkRefused.class,
            () -> store.fork(parent("open"), Optional.of(2), Optional.empty()));
    assertEquals(ForkCode.OPEN_TURN, refused.code());
  }

  /**
   * S4 — and the end-of-seed marker is not a turn marker, so a boundary landing on one is
   * allowed. The copied prefix already ends in a marker, so it is not marked again and the
   * child's seed length counts all three.
   */
  @Test
  void aBoundaryOnTheEndOfSeedMarkerIsAllowed() {
    var source =
        store.createSeeded(
            "marked",
            List.of(
                new SessionEvent(0, "turn/start", 1L, Map.of()),
                new SessionEvent(1, "turn/end", 2L, Map.of())));
    assertEquals("session/end-seed", source.events().get(2).type());

    var child = store.fork(source, Optional.of(2), Optional.of("marked-child"));
    assertEquals(3, child.seedLength());
    assertEquals(
        List.of("turn/start", "turn/end", "session/end-seed"),
        child.events().stream().map(SessionEvent::type).toList());
  }

  /**
   * S4 — the scan reads the last TURN marker, not the last event. A session interrupted
   * mid-turn is stored with a turn/start last, and reopening it puts the end-of-seed marker
   * after that start; a fork landing on the marker is still forking out of an open turn.
   */
  @Test
  void anOpenTurnBehindTheEndOfSeedMarkerIsStillAnOpenTurn() {
    var source =
        store.createSeeded(
            "cut-short", List.of(new SessionEvent(0, "turn/start", 1L, Map.of())));
    assertEquals(
        List.of("turn/start", "session/end-seed"),
        source.events().stream().map(SessionEvent::type).toList());

    assertEquals(
        ForkCode.OPEN_TURN, refusal(() -> store.fork(source, Optional.of(1), Optional.empty())));
  }

  /** S5 — five codes, each reachable and each distinguishable. */
  @Test
  void everyWayOfGettingAForkWrongHasItsOwnCode() {
    var source = parent("codes");
    store.create("taken");

    assertEquals(
        ForkCode.SESSION_ALREADY_EXISTS,
        refusal(() -> store.fork(source, Optional.of(1), Optional.of("taken"))));
    assertEquals(
        ForkCode.SESSION_NOT_FOUND, refusal(() -> store.forkById("nope", Optional.of(0), Optional.empty())));
    assertEquals(
        ForkCode.INVALID_BOUNDARY, refusal(() -> store.fork(source, Optional.of(99), Optional.empty())));
    assertEquals(
        ForkCode.INVALID_BOUNDARY, refusal(() -> store.fork(source, Optional.of(-1), Optional.empty())));
    assertEquals(
        ForkCode.SESSION_NOT_LIVE,
        refusal(() -> store.fork(SessionLog.create("codes"), Optional.of(0), Optional.empty())));
    assertEquals(
        ForkCode.OPEN_TURN, refusal(() -> store.fork(source, Optional.of(2), Optional.empty())));
  }

  /** S6 — an empty source forks to a marker, not to nothing. */
  @Test
  void forkingASessionWithNoEventsProducesAMarkerNotAnEmptyLog() {
    store.create("empty");
    var child = store.forkById("empty", Optional.empty(), Optional.empty());
    assertEquals(List.of("session/end-seed"), child.events().stream().map(SessionEvent::type).toList());
    assertEquals(0, child.seedLength());
    assertEquals(0, child.firstLiveSeq());
  }

  /** A forked child is a live session of the store like any other. */
  @Test
  void aForkedChildIsHeldByTheStore() {
    var child = store.fork(parent("held"), Optional.of(1), Optional.of("held-child"));
    assertEquals(child, store.get("held-child"));
    assertEquals(List.of("held", "held-child"), store.list().stream().map(SessionLog::id).toList());
  }

  private static ForkCode refusal(Runnable attempt) {
    return assertThrows(SessionForkRefused.class, attempt::run).code();
  }
}
