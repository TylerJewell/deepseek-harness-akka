package io.akka.dsh.application;

/** The ways a fork request can be wrong, each distinguishable by the caller. */
public enum ForkCode {
  SESSION_NOT_FOUND,
  SESSION_NOT_LIVE,
  SESSION_ALREADY_EXISTS,
  INVALID_BOUNDARY,
  OPEN_TURN
}
