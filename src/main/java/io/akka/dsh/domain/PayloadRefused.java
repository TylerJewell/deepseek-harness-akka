package io.akka.dsh.domain;

/**
 * A write the log declined, carrying the reason as a code the caller can act on rather
 * than only as a message. Refusal happens at the call, before anything is stored.
 */
public final class PayloadRefused extends RuntimeException {

  private final String code;

  public PayloadRefused(String code, String message) {
    super(message);
    this.code = code;
  }

  /** The stable reason: {@code NOT_JSON}, {@code SEED_NOT_CONTIGUOUS} or {@code LOG_FULL}. */
  public String code() {
    return code;
  }
}
