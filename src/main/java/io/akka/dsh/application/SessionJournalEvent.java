package io.akka.dsh.application;

import akka.javasdk.annotations.TypeName;
import io.akka.dsh.domain.SessionRecord;
import java.util.List;

/**
 * What a session persists.
 *
 * <p>Lives beside the entity rather than in {@code domain/}: it carries an Akka annotation,
 * and the domain package is kept free of the framework so its rules can be read and tested
 * without one.
 */
public sealed interface SessionJournalEvent {

  /** The session came into being, with whatever replayed history it was opened on. */
  @TypeName("session-opened")
  record SessionOpened(int seedLength, String parentSession, List<SessionRecord.StoredEvent> seed)
      implements SessionJournalEvent {}

  /** One accepted append. */
  @TypeName("event-appended")
  record EventAppended(SessionRecord.StoredEvent event) implements SessionJournalEvent {}
}
