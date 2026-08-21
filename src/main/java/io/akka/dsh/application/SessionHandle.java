package io.akka.dsh.application;

/**
 * A live session together with the capability to remove it. Removing it is what ends its
 * publication and announces its disposal; nothing else does.
 */
public final class SessionHandle {

  private final SessionStore store;
  private final SessionLog session;
  private boolean announced;
  private boolean removed;

  SessionHandle(SessionStore store, SessionLog session) {
    this.store = store;
    this.session = session;
  }

  public SessionLog session() {
    return session;
  }

  /** Remove the session from its store. Calling this again does nothing. */
  public void dispose() {
    if (removed) {
      return;
    }
    removed = true;
    store.remove(session, announced);
  }

  void markAnnounced() {
    announced = true;
  }
}
