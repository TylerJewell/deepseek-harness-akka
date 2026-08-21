package io.akka.dsh.application;

/**
 * Raised when a dispatch that waits for every listener had more than one of them fail.
 * Every failure is attached as a suppressed exception, in listener order, so the caller
 * sees all of them rather than whichever happened to be first.
 */
public final class DispatchFailed extends RuntimeException {

  DispatchFailed(String message) {
    super(message);
  }
}
