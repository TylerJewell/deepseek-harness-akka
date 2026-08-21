package io.akka.dsh.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.dsh.domain.PayloadRefused;
import io.akka.dsh.domain.PayloadValidator;
import io.akka.dsh.domain.SessionRecord;
import java.util.List;
import java.util.Optional;

/**
 * One session's durable log. The entity id is the session id.
 *
 * <p>The rules about what a log accepts live in {@link SessionLog} and {@link ForkPrefix}
 * and are tested without a runtime; this class decides only what to persist, and applies
 * the payload rule before persisting rather than after, so a value with no faithful JSON
 * spelling never reaches the journal.
 */
@Component(id = "session")
public class SessionEntity extends EventSourcedEntity<SessionRecord, SessionJournalEvent> {

  private final String sessionId;

  public SessionEntity(EventSourcedEntityContext context) {
    this.sessionId = context.entityId();
  }

  @Override
  public SessionRecord emptyState() {
    return SessionRecord.empty();
  }

  /** What a caller supplies to open a session. */
  public record Open(List<SessionRecord.StoredEvent> seed, String parentSession) {}

  /** What a caller supplies to append. */
  public record Append(String type, Object data) {}

  /**
   * Open the session, seeding it with replayed history when there is any. A seeded session
   * gains one end-of-seed marker, and a seed that already ends in one does not gain a
   * second — so reopening a session that was never touched does not grow its log.
   */
  public Effect<SessionRecord> open(Open request) {
    if (currentState().exists()) {
      return effects().error("session \"" + sessionId + "\" already exists");
    }
    var seed = request.seed() == null ? List.<SessionRecord.StoredEvent>of() : request.seed();
    List<SessionJournalEvent> persisted;
    try {
      persisted = openingEvents(seed, request.parentSession());
    } catch (PayloadRefused refused) {
      return effects().error(refused.code() + ": " + refused.getMessage());
    }
    return effects().persistAll(persisted).thenReply(state -> state);
  }

  private List<SessionJournalEvent> openingEvents(
      List<SessionRecord.StoredEvent> seed, String parentSession) {
    var validated = new java.util.ArrayList<SessionRecord.StoredEvent>();
    for (var index = 0; index < seed.size(); index++) {
      var event = seed.get(index);
      if (event.seq() != index) {
        throw new PayloadRefused(
            "SEED_NOT_CONTIGUOUS",
            "seed event at index " + index + " has seq " + event.seq()
                + "; a seed must be contiguous from 0");
      }
      validated.add(
          new SessionRecord.StoredEvent(
              index, event.type(), event.time(), PayloadValidator.snapshot(event.data())));
    }

    var opened =
        new SessionJournalEvent.SessionOpened(validated.size(), parentSession, List.copyOf(validated));
    var alreadyMarked =
        !validated.isEmpty()
            && SessionLog.END_SEED.equals(validated.get(validated.size() - 1).type());
    if (seed.isEmpty() && parentSession == null) {
      // A session opened with no replayed history at all has nothing to mark the end of.
      return List.of(opened);
    }
    if (alreadyMarked) {
      return List.of(opened);
    }
    return List.of(
        opened,
        new SessionJournalEvent.EventAppended(
            new SessionRecord.StoredEvent(
                validated.size(), SessionLog.END_SEED, System.currentTimeMillis(), java.util.Map.of())));
  }

  /**
   * Append one event, refusing at the call — before the journal changes — a payload with no
   * faithful JSON spelling, or a session already at its bound.
   */
  public Effect<SessionRecord.StoredEvent> append(Append request) {
    if (!currentState().exists()) {
      return effects().error("session \"" + sessionId + "\" does not exist");
    }
    if (currentState().seq() >= SessionLog.DEFAULT_MAX_EVENTS) {
      return effects()
          .error(
              "LOG_FULL: session \"" + sessionId + "\" already holds its limit of "
                  + SessionLog.DEFAULT_MAX_EVENTS + " events");
    }
    Object snapshot;
    try {
      snapshot = PayloadValidator.snapshot(request.data());
    } catch (PayloadRefused refused) {
      return effects().error(refused.code() + ": " + refused.getMessage());
    }
    var event =
        new SessionRecord.StoredEvent(
            currentState().seq(), request.type(), System.currentTimeMillis(), snapshot);
    return effects()
        .persist(new SessionJournalEvent.EventAppended(event))
        .thenReply(state -> event);
  }

  /** The whole durable log. */
  public ReadOnlyEffect<SessionRecord> get() {
    if (!currentState().exists()) {
      return effects().error("session \"" + sessionId + "\" does not exist");
    }
    return effects().reply(currentState());
  }

  /**
   * Everything after {@code afterSeq}. This is what a consumer that lost its connection
   * asks for: it names the last position it saw and is served the rest from the durable
   * log, so nothing that landed while it was away is skipped.
   */
  public ReadOnlyEffect<List<SessionRecord.StoredEvent>> since(int afterSeq) {
    if (!currentState().exists()) {
      return effects().error("session \"" + sessionId + "\" does not exist");
    }
    return effects().reply(currentState().since(afterSeq));
  }

  @Override
  public SessionRecord applyEvent(SessionJournalEvent event) {
    return switch (event) {
      case SessionJournalEvent.SessionOpened opened -> {
        var seeded = SessionRecord.empty();
        for (var stored : opened.seed()) {
          seeded = seeded.with(stored);
        }
        yield seeded.opened(opened.seedLength(), Optional.ofNullable(opened.parentSession()));
      }
      case SessionJournalEvent.EventAppended appended -> currentState().with(appended.event());
    };
  }
}
