package io.akka.dsh.application;

/** A listener's place in the bus, removable once. */
public interface Registration {
  void dispose();
}
