package io.akka.dsh.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * The durable form of a session: the events, and where the replayed part of them ends.
 *
 * <p>This is the state an entity rebuilds by replaying its journal, so it holds only what
 * survives a restart. The rules about what may enter it are in {@link PayloadValidator} and
 * are applied before an event is persisted, never after — a log is the source of truth, so
 * a bad event has to fail at the write rather than at a later read.
 *
 * <p>{@link #with} appends in place and returns the same record. Replay applies one event
 * at a time, so copying the whole list per event would make rebuilding an n-event session
 * cost n² element copies; the events themselves are immutable and {@link #events()} hands
 * out an unwritable view, so nothing outside can observe the difference.
 */
public record SessionRecord(
    List<StoredEvent> events, int seedLength, Optional<String> parentSession, boolean exists) {

  /** One durable event. Kept separate from the in-memory event so the journal's shape is
   * not tied to the runtime type. */
  public record StoredEvent(int seq, String type, long time, Object data) {}

  public SessionRecord {
    events = events instanceof ArrayList<StoredEvent> ? events : new ArrayList<>(events);
  }

  public static SessionRecord empty() {
    return new SessionRecord(List.of(), 0, Optional.empty(), false);
  }

  /** The next sequence number, which is the log's length. */
  public int seq() {
    return events.size();
  }

  /** An unwritable view of the log. */
  @Override
  public List<StoredEvent> events() {
    return Collections.unmodifiableList(events);
  }

  public SessionRecord opened(int seedLength, Optional<String> parentSession) {
    return new SessionRecord(events, seedLength, parentSession, true);
  }

  public SessionRecord with(StoredEvent event) {
    events.add(event);
    return this;
  }

  /** Events after {@code afterSeq}, which is how a consumer that dropped its connection
   * catches up: it names the last position it saw. */
  public List<StoredEvent> since(int afterSeq) {
    return events.stream().filter(event -> event.seq() > afterSeq).toList();
  }
}
