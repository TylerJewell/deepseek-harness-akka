package io.akka.dsh.application;

/** A fork the store declined, carrying which of the five ways it was wrong. */
public final class SessionForkRefused extends RuntimeException {

  private final ForkCode code;

  public SessionForkRefused(ForkCode code, String message) {
    super(message);
    this.code = code;
  }

  public ForkCode code() {
    return code;
  }
}
