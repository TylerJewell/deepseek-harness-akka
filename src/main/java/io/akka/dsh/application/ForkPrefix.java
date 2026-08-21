package io.akka.dsh.application;

import java.util.List;
import java.util.Optional;

/** Which events a fork copies, and every way of asking for the wrong ones. */
final class ForkPrefix {

  private static final String TURN_START = "turn/start";
  private static final String TURN_END = "turn/end";

  private ForkPrefix() {}

  /**
   * @param boundary inclusive source seq; empty means through the last event.
   * @return the prefix, empty when the source has no events and no boundary was named.
   */
  static List<SessionEvent> select(SessionLog source, Optional<Integer> boundary) {
    var events = source.events();
    if (boundary.isEmpty()) {
      return events.isEmpty() ? List.of() : List.copyOf(events);
    }
    var at = boundary.get();
    if (at < 0 || at >= events.size()) {
      throw new SessionForkRefused(
          ForkCode.INVALID_BOUNDARY,
          "boundary " + at + " does not exist in session \"" + source.id() + "\"");
    }
    var prefix = events.subList(0, at + 1);
    assertNoOpenTurn(source, prefix, at);
    return List.copyOf(prefix);
  }

  /**
   * A prefix may end between turns but not inside one. Only turn markers are read: the
   * end-of-seed marker is not one, so a boundary landing on it is looking past it at
   * whichever turn marker came before.
   */
  private static void assertNoOpenTurn(SessionLog source, List<SessionEvent> prefix, int at) {
    for (var index = prefix.size() - 1; index >= 0; index--) {
      var type = prefix.get(index).type();
      if (TURN_END.equals(type)) {
        return;
      }
      if (TURN_START.equals(type)) {
        throw new SessionForkRefused(
            ForkCode.OPEN_TURN,
            "boundary " + at + " in session \"" + source.id() + "\" ends inside an open turn");
      }
    }
  }
}
