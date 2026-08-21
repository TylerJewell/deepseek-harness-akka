package io.akka.dsh.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-001 L6 and L8 through L14 — what having a store around the log adds. */
class SessionStoreTest {

  private final EventBus bus = new EventBus();
  private final PluginScope scope = PluginScope.root();
  private final SessionStore store = new SessionStore(bus);

  /** L6 — one notification per append, in log order. */
  @Test
  void anAppendOnAHeldSessionNotifiesObserversOncePerAppendInLogOrder() {
    var seen = new ArrayList<Integer>();
    bus.on(scope, SessionStore.EVENT, args -> {
      seen.add(((SessionEvent) args[1]).seq());
      return null;
    });
    var s = store.create("pub");
    s.append("turn/start", Map.of("turn", 1L));
    s.append("turn/end", Map.of("turn", 1L));
    assertEquals(List.of(0, 1), seen);
  }

  /** L8 — the append is committed once the event is in the log; an observer failure
   * changes nothing about it. */
  @Test
  void anObserverThatFailsDoesNotUndoTheAppendOrStopLaterObservers() {
    var seen = new ArrayList<String>();
    bus.on(scope, SessionStore.EVENT, args -> {
      seen.add("first");
      throw new IllegalStateException("observer blew up");
    });
    bus.on(scope, SessionStore.EVENT, args -> {
      seen.add("second");
      return null;
    });
    var s = store.create("contained");
    var logged = s.append("turn/start", Map.of("turn", 1L));
    assertEquals(List.of("first", "second"), seen);
    assertEquals(0, logged.seq());
    assertEquals(1, s.events().size());
  }

  /** L9 — an append made while a notification is open is refused, and nothing lands. */
  @Test
  void anAppendFromInsideANotificationIsRefused() {
    var caught = new RuntimeException[1];
    var s = store.create("reenter");
    bus.on(bus_scope(), SessionStore.EVENT, args -> {
      caught[0] =
          assertThrows(
              IllegalStateException.class,
              () -> ((SessionLog) args[0]).append("turn/end", Map.of("turn", 1L)));
      return null;
    });
    s.append("turn/start", Map.of("turn", 1L));
    assertTrue(caught[0].getMessage().contains("reenter"));
    assertEquals(1, s.events().size());
  }

  /** L10 — one creation announcement, one removal announcement, in that order. */
  @Test
  void creationAndRemovalAreEachAnnouncedExactlyOnce() {
    var order = new ArrayList<String>();
    bus.on(scope, SessionStore.CREATED, args -> {
      order.add("created:" + ((SessionLog) args[0]).id());
      return null;
    });
    bus.on(scope, SessionStore.DISPOSED, args -> {
      order.add("disposed:" + ((SessionLog) args[0]).id());
      return null;
    });

    var handle = store.createOwned("owned");
    assertEquals(List.of("created:owned"), order);
    handle.dispose();
    handle.dispose();
    assertEquals(List.of("created:owned", "disposed:owned"), order);
    assertNull(store.get("owned"));
  }

  /** L11 — a failed creation announcement leaves nothing behind and still pairs. */
  @Test
  void aFailedCreationAnnouncementRollsBackAndStillAnnouncesRemoval() {
    var order = new ArrayList<String>();
    bus.on(scope, SessionStore.CREATED, args -> {
      order.add("created");
      throw new IllegalStateException("veto");
    });
    bus.on(scope, SessionStore.DISPOSED, args -> {
      order.add("disposed");
      return null;
    });

    var failure = assertThrows(IllegalStateException.class, () -> store.create("rolled-back"));
    assertEquals("veto", failure.getMessage());
    assertEquals(List.of("created", "disposed"), order);
    assertNull(store.get("rolled-back"));
  }

  /** A session removed by a rollback leaves its id free for the next attempt. */
  @Test
  void anIdFreedByARollbackCanBeUsedAgain() {
    var vetoing = bus.on(scope, SessionStore.CREATED, args -> {
      throw new IllegalStateException("veto");
    });
    assertThrows(IllegalStateException.class, () -> store.create("retried"));
    vetoing.dispose();
    assertEquals("retried", store.create("retried").id());
  }

  /** L12 — no two live sessions share an id, and listing is creation-ordered. */
  @Test
  void aDuplicateIdIsRefusedAndListingReportsCreationOrder() {
    store.create("a");
    store.create("b");
    var refused = assertThrows(IllegalStateException.class, () -> store.create("a"));
    assertTrue(refused.getMessage().contains("already exists"));
    assertEquals(List.of("a", "b"), store.list().stream().map(SessionLog::id).toList());
  }

  /** L13 — a minted id skips one already taken rather than colliding with it. */
  @Test
  void anOmittedIdIsMintedSkippingIdsAlreadyTaken() {
    assertEquals("session-1", store.create().id());
    store.create("session-2");
    assertEquals("session-3", store.create().id());
  }

  /** L14 — every listener settles first; only then is the first failure raised. */
  @Test
  void flushSettlesEveryListenerThenRaisesTheFirstFailure() {
    var settled = java.util.Collections.synchronizedList(new ArrayList<String>());
    bus.on(scope, SessionStore.FLUSH, args -> {
      throw new IllegalStateException("backend down");
    });
    bus.on(scope, SessionStore.FLUSH, args -> {
      try {
        Thread.sleep(40);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      settled.add("slow");
      return null;
    });
    var s = store.create("flush-fail");
    var failure = assertThrows(DispatchFailed.class, () -> store.flush(s));
    assertEquals("backend down", failure.getSuppressed()[0].getMessage());
    assertEquals(List.of("slow"), List.copyOf(settled));
  }

  /** L14 — the answer is participation, not durability. */
  @Test
  void flushReportsWhetherAnyListenerTookPart() {
    var s = store.create("flush-none");
    assertEquals(false, store.flush(s));
    bus.on(scope, SessionStore.FLUSH, args -> null);
    assertEquals(true, store.flush(s));
  }

  /** L14 — a session this store does not hold cannot be flushed through it. */
  @Test
  void flushRefusesASessionTheStoreDoesNotHold() {
    var detached = SessionLog.create("not-live");
    var refused = assertThrows(IllegalStateException.class, () -> store.flush(detached));
    assertTrue(refused.getMessage().contains("not live"));
  }

  /** A removed session is no longer notified about, because it is no longer held. */
  @Test
  void aDisposedSessionStopsBeingNotifiedAbout() {
    var seen = new ArrayList<Integer>();
    bus.on(scope, SessionStore.EVENT, args -> {
      seen.add(((SessionEvent) args[1]).seq());
      return null;
    });
    var handle = store.createOwned("temporary");
    handle.session().append("turn/start", Map.of("turn", 1L));
    handle.dispose();
    handle.session().append("turn/end", Map.of("turn", 1L));
    assertEquals(List.of(0), seen);
  }

  private PluginScope bus_scope() {
    return scope;
  }
}
