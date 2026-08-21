package io.akka.dsh.application;

/** A listener for the modes that pass results back: its return value is read by
 * {@link io.akka.dsh.domain.BailRule}. */
@FunctionalInterface
public interface Listener {
  Object call(Object... args);
}
