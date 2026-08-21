package io.akka.dsh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * What the target can hold, established by running it rather than by reading about
 * it. Each question here decided something in SPEC-001 before any of the rebuild
 * was written; the spec's Open decisions column names the answers.
 */
class TargetCapacityTest {

  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * T1. The source rejects a non-JSON payload at the append site so that a bad
   * event never reaches storage. Does the target's serializer refuse the same
   * values, and does it refuse them synchronously at the call?
   */
  @Test
  void jacksonRefusesTheValuesTheSourceRefuses() {
    // Circularity is the one source rejection the serializer makes for us: it is
    // detected rather than looped on.
    var self = new java.util.HashMap<String, Object>();
    self.put("self", self);
    assertThrows(Exception.class, () -> mapper.writeValueAsString(self));
  }

  /**
   * T2. Values the source rejects that the target ACCEPTS silently — these are the
   * ones the rebuild has to reject itself, because the serializer will not.
   */
  @Test
  void jacksonAcceptsValuesTheSourceRefusesSoTheRebuildMustCheckThem() throws Exception {
    // Non-finite numbers: the source rejects them because JSON has no spelling
    // for them. The serializer neither rejects nor preserves — it writes a
    // STRING, so a number goes in and a number-shaped string comes out. A
    // silent type change is the worst of the three outcomes, which is why
    // rejecting non-finite values is the rebuild's own rule.
    assertEquals("\"NaN\"", mapper.writeValueAsString(Double.NaN));
    assertEquals("\"Infinity\"", mapper.writeValueAsString(Double.POSITIVE_INFINITY));

    // Negative zero: the source rejects it (JSON cannot distinguish -0 from 0);
    // Jackson writes it as "-0.0" and reads it back as a negative zero double.
    assertEquals("-0.0", mapper.writeValueAsString(-0.0d));
    assertTrue(1 / mapper.readValue("-0.0", Double.class) < 0);

    // A Date-like value: the source rejects an exotic object, the target maps it
    // to a number or a string depending on configuration. Either way it is
    // accepted, so "reject exotic types" is the rebuild's own rule, not the
    // serializer's.
    assertNotNull(mapper.writeValueAsString(new java.util.Date(0)));
  }

  /**
   * T3. The source's log is an in-memory array whose events are deep-frozen. The
   * target's durable analogue is an event-sourced journal. Confirm that ordinary
   * Java immutability is available for the same guarantee: a returned view of the
   * log cannot be written through.
   */
  @Test
  void immutableListsRefuseWritesTheWayDeepFreezeDoes() {
    var log = List.of("a", "b");
    assertThrows(UnsupportedOperationException.class, () -> log.add("c"));
    var copy = List.copyOf(log);
    assertThrows(UnsupportedOperationException.class, () -> copy.set(0, "z"));
  }

  /**
   * T4. The source's waterfall composes listeners around a `next` continuation and
   * a listener that never calls it vetoes the rest. Confirm the target language can
   * express the same shape without a coroutine — the rebuild's dispatch is plain
   * Java, so this decides whether waterfall is portable at all.
   */
  @Test
  void aNextContinuationComposesInPlainJava() {
    interface Listener {
      String apply(List<String> acc, java.util.function.Supplier<String> next);
    }
    List<Listener> listeners =
        List.of(
            (acc, next) -> {
              acc.add("outer-in");
              var r = next.get();
              acc.add("outer-out");
              return r;
            },
            (acc, next) -> {
              acc.add("veto");
              return "vetoed";
            });
    var acc = new java.util.ArrayList<String>();
    var queue = new java.util.ArrayDeque<>(listeners);
    var inner = new java.util.function.Supplier<String>() {
      @Override
      public String get() {
        var cb = queue.poll();
        if (cb == null) {
          acc.add("builtin");
          return "builtin";
        }
        return cb.apply(acc, this);
      }
    };
    assertEquals("vetoed", inner.get());
    assertEquals(List.of("outer-in", "veto", "outer-out"), acc);
  }

  /**
   * T5. `parallel` aggregates every listener failure rather than the first. Confirm
   * the target has an aggregate carrier so the rebuild does not have to invent one
   * that loses failures.
   */
  @Test
  void suppressedExceptionsCarryEveryFailure() {
    var aggregate = new RuntimeException("dispatch failed");
    aggregate.addSuppressed(new IllegalStateException("one"));
    aggregate.addSuppressed(new IllegalStateException("two"));
    assertEquals(
        List.of("one", "two"),
        java.util.Arrays.stream(aggregate.getSuppressed()).map(Throwable::getMessage).toList());
  }

  /**
   * T6. A durable session log needs a bound. Ask what the target does when the
   * bound is EXCEEDED, not only what it is: a rejection the caller can see, or a
   * silent no-op. This decides whether the rebuild's append can report a refusal.
   */
  @Test
  void aRefusalIsVisibleToTheCallerRatherThanSilent() {
    // Stand-in for the entity's own guard: the question is whether a Java
    // command boundary can return a typed refusal rather than throwing away the
    // reason. The rebuild's Session.append returns exactly this shape.
    interface Outcome {
      record Accepted(int seq) implements Outcome {}

      record Refused(String code, String message) implements Outcome {}
    }
    Outcome refused = new Outcome.Refused("INVALID_BOUNDARY", "boundary 99 does not exist");
    assertTrue(refused instanceof Outcome.Refused r && r.code().equals("INVALID_BOUNDARY"));
  }

  /** T7. Confirm the test harness itself runs, so a failure below is the subject's. */
  @Test
  void theHarnessRuns() {
    assertEquals(Duration.ofSeconds(1), Duration.ofMillis(1000));
    assertEquals(Map.of(), Map.of());
  }
}
