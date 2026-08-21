package io.akka.dsh.application;

import io.akka.dsh.domain.PayloadRefused;
import io.akka.dsh.domain.PayloadValidator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A session's append-only event log.
 *
 * <p>The log assigns every sequence number from its own length, so it is contiguous from 0
 * and a caller cannot choose where an event lands. A payload is validated and copied in the
 * same pass, so what is stored is detached from what the caller passed and cannot be
 * written through afterwards.
 *
 * <p>A log works with no store around it: it accepts appends and notifies nobody.
 * Publication is what a store adds.
 */
public final class SessionLog {

  /** The marker separating replayed history from what this process appended. */
  public static final String END_SEED = "session/end-seed";

  /**
   * The bound on a durable log. The source has none — its log lives in a process that exits
   * — so this is the rebuild's own rule, and exceeding it is a refusal the caller sees
   * rather than a write that quietly does not happen.
   *
   * <p>Set from the target's own ceiling rather than picked: entity state must stay under
   * 1 MB to replicate, and this state IS the whole log, so at a nominal 100 bytes per event
   * a thousand events is the order where that ceiling starts to matter.
   */
  public static final int DEFAULT_MAX_EVENTS = 1_000;

  private final String id;
  private final int maxEvents;
  private final List<SessionEvent> log = new ArrayList<>();
  private final int firstLiveSeq;
  private final int seedLength;
  private String parentSession;

  private List<SessionEvent> view;
  private Publication publication;
  private boolean appending;

  /** How a store hears about an accepted event. */
  interface Publication {
    void publish(SessionLog session, SessionEvent event);

    int observerCount();
  }

  private SessionLog(String id, int maxEvents, List<SessionEvent> seed, boolean seeded) {
    this.id = id;
    this.maxEvents = maxEvents;
    if (seeded) {
      adopt(seed);
    }
    this.seedLength = log.size();
    // Read before the marker is appended. Where this construction appends one, the marker
    // occupies this seq itself; where the seed already ended in one, this is just past it.
    // Either way it is the first seq a live append can take.
    this.firstLiveSeq = log.size();
    if (seeded && (log.isEmpty() || !END_SEED.equals(log.get(log.size() - 1).type()))) {
      appendAccepted(END_SEED, PayloadValidator.snapshot(java.util.Map.of()));
    }
  }

  /** A session with no replayed history. */
  public static SessionLog create(String id) {
    return new SessionLog(id, DEFAULT_MAX_EVENTS, List.of(), false);
  }

  /** A session with no replayed history and a bound of {@code maxEvents}. */
  public static SessionLog createBounded(String id, int maxEvents) {
    return new SessionLog(id, maxEvents, List.of(), false);
  }

  /**
   * A session constructed from replayed history. The seed is held to the same rules a live
   * append is held to, and is ended with one marker.
   */
  public static SessionLog seeded(String id, List<SessionEvent> seed) {
    return new SessionLog(id, DEFAULT_MAX_EVENTS, seed, true);
  }

  private void adopt(List<SessionEvent> seed) {
    for (var index = 0; index < seed.size(); index++) {
      var event = seed.get(index);
      if (event.seq() != index) {
        throw new PayloadRefused(
            "SEED_NOT_CONTIGUOUS",
            "seed event at index " + index + " has seq " + event.seq()
                + "; a seed must be contiguous from 0");
      }
      log.add(
          new SessionEvent(index, event.type(), event.time(), PayloadValidator.snapshot(event.data())));
    }
  }

  public String id() {
    return id;
  }

  /** The next event's sequence number, which is the log's length. */
  public int seq() {
    return log.size();
  }

  /** The first sequence number appended in this process; the seed ends just below it. */
  public int firstLiveSeq() {
    return firstLiveSeq;
  }

  /** How many events entered through construction rather than a live append. */
  public int seedLength() {
    return seedLength;
  }

  /** The session this one was forked from, when it was forked from one. */
  public Optional<String> parentSession() {
    return Optional.ofNullable(parentSession);
  }

  /**
   * An unwritable view of the log. The view is reused until the next append, so a list
   * handed to a caller earlier does not grow later.
   */
  public List<SessionEvent> events() {
    if (view == null) {
      view = List.copyOf(log);
    }
    return view;
  }

  int observerCount() {
    return publication == null ? 0 : publication.observerCount();
  }

  /**
   * Append one event.
   *
   * <p>The payload is refused if it has no faithful JSON spelling, and the log is bounded;
   * both refusals happen here, before the log changes. Once the event is in the log the
   * append is committed: an observer that fails does not undo it and does not stop the
   * observers after it.
   *
   * @return the accepted event, with its assigned position and the payload as stored.
   */
  public SessionEvent append(String type, Object data) {
    if (appending) {
      throw new IllegalStateException(
          "session \"" + id + "\": an append cannot reenter while another is being published");
    }
    if (log.size() >= maxEvents) {
      throw new PayloadRefused(
          "LOG_FULL", "session \"" + id + "\" already holds its limit of " + maxEvents + " events");
    }
    var snapshot = PayloadValidator.snapshot(data);
    return publish(appendAccepted(type, snapshot));
  }

  private SessionEvent appendAccepted(String type, Object snapshot) {
    var event = new SessionEvent(log.size(), type, System.currentTimeMillis(), snapshot);
    log.add(event);
    view = null;
    return event;
  }

  private SessionEvent publish(SessionEvent event) {
    if (publication == null) {
      return event;
    }
    appending = true;
    try {
      publication.publish(this, event);
    } finally {
      appending = false;
    }
    return event;
  }

  void attach(Publication publication) {
    this.publication = publication;
  }

  void detach() {
    this.publication = null;
  }

  void recordParent(String parentSessionId) {
    this.parentSession = parentSessionId;
  }
}
