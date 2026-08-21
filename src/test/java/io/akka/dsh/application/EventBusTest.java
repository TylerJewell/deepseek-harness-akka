package io.akka.dsh.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** SPEC-001 D1 through D8 — the five dispatch modes and listener placement. */
class EventBusTest {

  private final EventBus bus = new EventBus();
  private final PluginScope scope = PluginScope.root();

  /** D1 — emit hands the event to every listener and returns; a listener that finishes
   * later finishes later. */
  @Test
  void emitDoesNotWaitForAnAsynchronousListener() throws Exception {
    var order = java.util.Collections.synchronizedList(new ArrayList<String>());
    var released = new CountDownLatch(1);
    var finished = new CountDownLatch(1);
    bus.on(scope, "probe", args -> {
      new Thread(() -> {
        try {
          released.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        order.add("later");
        finished.countDown();
      }).start();
      return null;
    });
    bus.on(scope, "probe", args -> {
      order.add("immediate");
      return null;
    });

    bus.emit("probe");
    order.add("after-emit");
    assertEquals(List.of("immediate", "after-emit"), List.copyOf(order));

    released.countDown();
    assertTrue(finished.await(5, TimeUnit.SECONDS));
    assertEquals(List.of("immediate", "after-emit", "later"), List.copyOf(order));
  }

  /** D2 — parallel waits for all of them and loses none of the failures. */
  @Test
  void parallelWaitsForEveryListenerAndReportsEveryFailure() {
    var settled = java.util.Collections.synchronizedList(new ArrayList<String>());
    bus.on(scope, "probe", args -> {
      throw new IllegalStateException("one");
    });
    bus.on(scope, "probe", args -> {
      try {
        Thread.sleep(40);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      settled.add("slow");
      return null;
    });
    bus.on(scope, "probe", args -> {
      throw new IllegalStateException("two");
    });

    var failure = assertThrows(DispatchFailed.class, () -> bus.parallel("probe"));
    assertEquals(
        List.of("one", "two"),
        Arrays.stream(failure.getSuppressed()).map(Throwable::getMessage).toList());
    assertEquals(List.of("slow"), List.copyOf(settled));
  }

  /** D4 — bail stops at the first stopping result and calls nothing after it. */
  @Test
  void bailStopsAtTheFirstStoppingResult() {
    var seen = new ArrayList<String>();
    bus.on(scope, "probe", args -> {
      seen.add("a");
      return null;
    });
    bus.on(scope, "probe", args -> {
      seen.add("b");
      return false;
    });
    bus.on(scope, "probe", args -> {
      seen.add("c");
      return 0;
    });
    bus.on(scope, "probe", args -> {
      seen.add("d");
      return "never reached";
    });

    assertEquals(0, bus.bail("probe"));
    assertEquals(List.of("a", "b", "c"), seen);
  }

  /** D4 — serial applies the same rule, waiting for each listener in turn. */
  @Test
  void serialStopsAtTheFirstStoppingResultAfterWaiting() {
    var seen = java.util.Collections.synchronizedList(new ArrayList<String>());
    bus.on(scope, "probe", args -> {
      sleep(20);
      seen.add("a");
      return null;
    });
    bus.on(scope, "probe", args -> {
      sleep(20);
      seen.add("b");
      return "stop";
    });
    bus.on(scope, "probe", args -> {
      seen.add("c");
      return null;
    });

    assertEquals("stop", bus.serial("probe"));
    assertEquals(List.of("a", "b"), List.copyOf(seen));
  }

  /** Nothing stopping means nothing returned, in both stopping modes. */
  @Test
  void aDispatchNoListenerStopsReturnsNothing() {
    bus.on(scope, "probe", args -> null);
    bus.on(scope, "probe", args -> false);
    assertNull(bus.bail("probe"));
    assertNull(bus.serial("probe"));
  }

  /** D5 — outermost first, and a listener that never continues stops the built-in too. */
  @Test
  void waterfallRunsOutermostFirstAndASkippedContinuationVetoesTheBuiltIn() {
    var acc = new ArrayList<String>();
    bus.onWaterfall(scope, "probe", (args, next) -> {
      acc.add("outer-in");
      var r = next.get();
      acc.add("outer-out");
      return r;
    });
    bus.onWaterfall(scope, "probe", (args, next) -> {
      acc.add("veto");
      return "vetoed";
    });
    bus.onWaterfall(scope, "probe", (args, next) -> {
      acc.add("never");
      return next.get();
    });

    var result =
        bus.waterfall("probe", () -> {
          acc.add("builtin");
          return "builtin";
        });

    assertEquals("vetoed", result);
    assertEquals(List.of("outer-in", "veto", "outer-out"), acc);
  }

  /** D5 — with every listener continuing, the built-in behaviour runs last. */
  @Test
  void waterfallReachesTheBuiltInWhenEveryListenerContinues() {
    var acc = new ArrayList<String>();
    bus.onWaterfall(scope, "probe", (args, next) -> {
      acc.add("one");
      return next.get();
    });
    bus.onWaterfall(scope, "probe", (args, next) -> {
      acc.add("two");
      return next.get();
    });
    assertEquals(
        "B",
        bus.waterfall("probe", () -> {
          acc.add("builtin");
          return "B";
        }));
    assertEquals(List.of("one", "two", "builtin"), acc);
  }

  /**
   * D6 — placement is the same rule in the composing mode, where it decides which listener
   * gets to veto the others rather than only what order they run in.
   */
  @Test
  void aPrependedWaterfallListenerWrapsTheOnesAlreadyRegistered() {
    var acc = new ArrayList<String>();
    bus.onWaterfall(scope, "probe", (args, next) -> {
      acc.add("outer-in");
      var r = next.get();
      acc.add("outer-out");
      return r;
    });
    bus.onWaterfall(scope, "probe", (args, next) -> {
      acc.add("veto");
      return "vetoed";
    });
    bus.onWaterfallPrepend(scope, "probe", (args, next) -> {
      acc.add("first-in");
      var r = next.get();
      acc.add("first-out");
      return r;
    });

    assertEquals("vetoed", bus.waterfall("probe", () -> "builtin"));
    assertEquals(List.of("first-in", "outer-in", "veto", "outer-out", "first-out"), acc);
  }

  /** D6 — registration order, and prepend goes ahead of everything already there. */
  @Test
  void listenersRunInRegistrationOrderAndPrependGoesFirst() {
    var seen = new ArrayList<String>();
    bus.on(scope, "probe", args -> {
      seen.add("first");
      return null;
    });
    bus.on(scope, "probe", args -> {
      seen.add("second");
      return null;
    });
    bus.onPrepend(scope, "probe", args -> {
      seen.add("prepended");
      return null;
    });
    bus.emit("probe");
    assertEquals(List.of("prepended", "first", "second"), seen);
  }

  /**
   * D7 — the registration is removed before the body runs, so a listener that dispatches
   * the same event from inside itself does not call itself again.
   */
  @Test
  void onceFiresAtMostOnceEvenWhenReenteredFromItsOwnBody() {
    var count = new int[1];
    bus.once(scope, "probe", args -> {
      count[0]++;
      if (count[0] < 5) {
        bus.emit("probe");
      }
      return null;
    });
    bus.emit("probe");
    bus.emit("probe");
    assertEquals(1, count[0]);
  }

  /** D8 — disposing the registration removes the listener; disposing again is harmless. */
  @Test
  void disposingARegistrationRemovesTheListener() {
    var seen = new ArrayList<String>();
    var registration = bus.on(scope, "probe", args -> {
      seen.add("x");
      return null;
    });
    bus.emit("probe");
    registration.dispose();
    registration.dispose();
    bus.emit("probe");
    assertEquals(List.of("x"), seen);
  }

  /** The dispatch arguments reach every listener unchanged. */
  @Test
  void listenersReceiveTheDispatchArguments() {
    var seen = new ArrayList<Object>();
    bus.on(scope, "probe", args -> {
      seen.addAll(Arrays.asList(args));
      return null;
    });
    bus.emit("probe", "a", 1L);
    assertEquals(List.of("a", 1L), seen);
  }

  /** An event nobody listens for is not an error in any mode. */
  @Test
  void anEventWithNoListenersIsNotAnError() {
    bus.emit("nobody");
    bus.parallel("nobody");
    assertNull(bus.bail("nobody"));
    assertNull(bus.serial("nobody"));
    assertEquals("builtin", bus.waterfall("nobody", () -> "builtin"));
  }

  /**
   * emit does not contain a listener failure: unlike an observer of an accepted session
   * event (L8), a plain dispatch has nothing committed to protect, so the failure
   * reaches the caller.
   */
  @Test
  void emitLetsAListenerFailureReachTheCaller() {
    bus.on(scope, "probe", args -> {
      throw new IllegalStateException("boom");
    });
    assertEquals("boom", assertThrows(IllegalStateException.class, () -> bus.emit("probe")).getMessage());
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
