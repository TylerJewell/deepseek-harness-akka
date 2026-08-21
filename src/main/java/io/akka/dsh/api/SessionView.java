package io.akka.dsh.api;

import io.akka.dsh.domain.SessionRecord;
import java.util.List;

/**
 * What a caller of the HTTP surface sees. Separate from {@link SessionRecord} so the
 * durable shape can change — an added state field, a different lineage representation —
 * without changing what callers already parse.
 *
 * @param events the log, oldest first.
 * @param seq the next position, which is the log's length.
 * @param seedLength how many events entered through replayed history.
 * @param parentSession the session this one was forked from, or null when it was not.
 */
public record SessionView(
    List<EventView> events, int seq, int seedLength, String parentSession) {

  /** One event as a caller sees it. */
  public record EventView(int seq, String type, long time, Object data) {

    static EventView of(SessionRecord.StoredEvent stored) {
      return new EventView(stored.seq(), stored.type(), stored.time(), stored.data());
    }
  }

  static SessionView of(SessionRecord record) {
    return new SessionView(
        record.events().stream().map(EventView::of).toList(),
        record.seq(),
        record.seedLength(),
        record.parentSession().orElse(null));
  }

  static List<EventView> of(List<SessionRecord.StoredEvent> events) {
    return events.stream().map(EventView::of).toList();
  }
}
