package io.akka.dsh.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.dsh.domain.SessionRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The entity's own decisions, without a runtime. The integration suite checks that these
 * survive a real journal; this one checks the decisions themselves, and does it in
 * milliseconds rather than seconds, so the rules can be exercised at a density the
 * runtime-backed suite could not afford.
 */
class SessionEntityTest {

  private static EventSourcedTestKit<SessionRecord, SessionJournalEvent, SessionEntity> kit(
      String id) {
    return EventSourcedTestKit.of(id, SessionEntity::new);
  }

  /** Opening with no history persists one event and marks nothing. */
  @Test
  void openingWithoutHistoryPersistsOneEventAndNoMarker() {
    var kit = kit("plain");
    var result = kit.method(SessionEntity::open).invoke(new SessionEntity.Open(List.of(), null));
    assertTrue(result.isReply());
    assertEquals(1, result.getAllEvents().size());
    assertEquals(0, kit.getState().seq());
    assertTrue(kit.getState().exists());
  }

  /** Replay rebuilds the same log the writes produced. */
  @Test
  void replayingTheJournalRebuildsTheSameLog() {
    var kit = kit("replayed");
    kit.method(SessionEntity::open).invoke(new SessionEntity.Open(List.of(), null));
    kit.method(SessionEntity::append).invoke(new SessionEntity.Append("turn/start", Map.of("turn", 1)));
    kit.method(SessionEntity::append).invoke(new SessionEntity.Append("turn/end", Map.of("turn", 1)));

    var replayed = kit.getState();
    assertEquals(List.of(0, 1), replayed.events().stream().map(SessionRecord.StoredEvent::seq).toList());
    assertEquals(
        List.of("turn/start", "turn/end"),
        replayed.events().stream().map(SessionRecord.StoredEvent::type).toList());
  }

  /** A payload with no faithful JSON spelling is refused without persisting anything. */
  @Test
  void aPayloadWithNoJsonSpellingIsRefusedWithoutPersisting() {
    var kit = kit("refusing");
    kit.method(SessionEntity::open).invoke(new SessionEntity.Open(List.of(), null));

    var refused =
        kit.method(SessionEntity::append)
            .invoke(new SessionEntity.Append("turn/start", Map.of("value", Double.NaN)));

    assertTrue(refused.isError());
    assertTrue(refused.getError().contains("NOT_JSON"));
    assertEquals(0, kit.getState().seq());
  }

  /** The bound is enforced at the entity, and exceeding it is a refusal the caller reads. */
  @Test
  void appendPastTheBoundIsRefusedAtTheEntity() {
    var kit = kit("bounded");
    kit.method(SessionEntity::open).invoke(new SessionEntity.Open(List.of(), null));
    for (var n = 0; n < SessionLog.DEFAULT_MAX_EVENTS; n++) {
      kit.method(SessionEntity::append).invoke(new SessionEntity.Append("turn/start", Map.of("n", n)));
    }
    assertEquals(SessionLog.DEFAULT_MAX_EVENTS, kit.getState().seq());

    var refused =
        kit.method(SessionEntity::append).invoke(new SessionEntity.Append("turn/end", Map.of()));
    assertTrue(refused.isError());
    assertTrue(refused.getError().contains("LOG_FULL"));
    assertEquals(SessionLog.DEFAULT_MAX_EVENTS, kit.getState().seq());
  }

  /** A resuming consumer is served everything after the position it names. */
  @Test
  void sinceServesEverythingAfterTheNamedPosition() {
    var kit = kit("resumed");
    kit.method(SessionEntity::open).invoke(new SessionEntity.Open(List.of(), null));
    for (var n = 0; n < 5; n++) {
      kit.method(SessionEntity::append).invoke(new SessionEntity.Append("turn/start", Map.of("n", n)));
    }

    assertEquals(
        List.of(2, 3, 4),
        kit.method(SessionEntity::since).invoke(1).getReply().stream()
            .map(SessionRecord.StoredEvent::seq)
            .toList());
    assertEquals(List.of(), kit.method(SessionEntity::since).invoke(4).getReply());
  }

  /** Appending to a session that was never opened is refused. */
  @Test
  void appendingToAnUnopenedSessionIsRefused() {
    var refused =
        kit("never-opened")
            .method(SessionEntity::append)
            .invoke(new SessionEntity.Append("turn/start", Map.of()));
    assertTrue(refused.isError());
    assertTrue(refused.getError().contains("does not exist"));
  }
}
