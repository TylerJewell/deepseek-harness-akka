package io.akka.dsh.application;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Holds the live sessions and publishes what happens to them.
 *
 * <p>Durability is not implemented here: a plugin subscribes to {@link #EVENT} to write
 * events out and to {@link #FLUSH} to be asked when they must have reached storage.
 */
public final class SessionStore {

  /** One accepted append, carrying the session and the event. */
  public static final String EVENT = "session/event";

  /** A session has entered the store. */
  public static final String CREATED = "session/created";

  /** A session that was announced has left the store. */
  public static final String DISPOSED = "session/disposed";

  /** Buffered events must reach durable storage. */
  public static final String FLUSH = "session/flush";

  private final EventBus bus;
  private final Map<String, SessionLog> live = new LinkedHashMap<>();
  private int counter;

  public SessionStore(EventBus bus) {
    this.bus = bus;
  }

  /** Create a session with a minted id. */
  public SessionLog create() {
    return create(mintId());
  }

  /** Create a session and announce it. */
  public SessionLog create(String id) {
    return createOwned(id).session();
  }

  /** Create a session from replayed history and announce it. */
  public SessionLog createSeeded(String id, List<SessionEvent> seed) {
    return enterAndAnnounce(SessionLog.seeded(id, seed)).session();
  }

  /**
   * Create a session and hand back the way to remove it. Removing it is the only thing
   * that ends the session's publication.
   */
  public SessionHandle createOwned(String id) {
    return enterAndAnnounce(SessionLog.create(id));
  }

  private SessionHandle enterAndAnnounce(SessionLog session) {
    var handle = enter(session);
    // Marked announced before the announcement runs. A listener may see the creation and
    // then a later listener may fail; the rollback still has to pair that partial
    // creation with a removal, which it can only do for a session it considers announced.
    handle.markAnnounced();
    try {
      bus.emit(CREATED, session);
    } catch (RuntimeException announcementFailed) {
      handle.dispose();
      throw announcementFailed;
    }
    return handle;
  }

  private SessionHandle enter(SessionLog session) {
    if (live.containsKey(session.id())) {
      throw new IllegalStateException("session \"" + session.id() + "\" already exists");
    }
    live.put(session.id(), session);
    session.attach(
        new SessionLog.Publication() {
          @Override
          public void publish(SessionLog owner, SessionEvent event) {
            // The event is already in the log, so the append is committed. A failing
            // observer is contained here rather than being allowed to change what the
            // appending caller was told.
            for (var observer : List.copyOf(observers())) {
              try {
                observer.call(owner, event);
              } catch (RuntimeException contained) {
                // Contained on purpose: see above.
              }
            }
          }

          @Override
          public int observerCount() {
            return observers().size();
          }

          private List<Listener> observers() {
            return bus.listenersOf(EVENT);
          }
        });
    return new SessionHandle(this, session);
  }

  void remove(SessionLog session, boolean announced) {
    if (live.get(session.id()) != session) {
      return;
    }
    live.remove(session.id());
    session.detach();
    if (announced) {
      bus.emit(DISPOSED, session);
    }
  }

  /** A live session, or nothing. */
  public SessionLog get(String id) {
    return live.get(id);
  }

  /** Live sessions, in creation order. */
  public List<SessionLog> list() {
    return new ArrayList<>(live.values());
  }

  /**
   * Ask every durability listener to make the session's events durable.
   *
   * <p>Every listener settles before anything is reported, so a slow one is not abandoned
   * because a fast one failed.
   *
   * @return whether any listener took part at all — participation, not a durability proof.
   */
  public boolean flush(SessionLog session) {
    assertLive(session);
    var listeners = bus.listenersOf(FLUSH);
    bus.parallel(FLUSH, session);
    return !listeners.isEmpty();
  }

  private void assertLive(SessionLog session) {
    if (live.get(session.id()) != session) {
      throw new IllegalStateException("session \"" + session.id() + "\" is not live in this store");
    }
  }

  /** Fork a child from a stable prefix of a live session named by id. */
  public SessionLog forkById(String sourceId, Optional<Integer> boundary, Optional<String> childId) {
    var source = live.get(sourceId);
    if (source == null) {
      throw new SessionForkRefused(
          ForkCode.SESSION_NOT_FOUND, "session \"" + sourceId + "\" not found");
    }
    return fork(source, boundary, childId);
  }

  /**
   * Fork a child from a stable prefix of a live session.
   *
   * @param boundary inclusive source seq; empty means through the source's last event.
   * @param childId the child's id; empty mints one.
   */
  public SessionLog fork(SessionLog source, Optional<Integer> boundary, Optional<String> childId) {
    if (childId.isPresent() && live.containsKey(childId.get())) {
      throw new SessionForkRefused(
          ForkCode.SESSION_ALREADY_EXISTS, "session \"" + childId.get() + "\" already exists");
    }
    var seed = ForkPrefix.select(resolve(source), boundary);
    var child = SessionLog.seeded(childId.orElseGet(this::mintId), seed);
    child.recordParent(source.id());
    return enterAndAnnounce(child).session();
  }

  private SessionLog resolve(SessionLog source) {
    var found = live.get(source.id());
    if (found == null) {
      throw new SessionForkRefused(
          ForkCode.SESSION_NOT_FOUND, "session \"" + source.id() + "\" not found");
    }
    if (found != source) {
      throw new SessionForkRefused(
          ForkCode.SESSION_NOT_LIVE,
          "session \"" + source.id() + "\" is not the live instance held by this store");
    }
    return source;
  }

  private String mintId() {
    String minted;
    do {
      minted = "session-" + (++counter);
    } while (live.containsKey(minted));
    return minted;
  }
}
