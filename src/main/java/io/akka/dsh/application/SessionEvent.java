package io.akka.dsh.application;

/**
 * One entry in a session's log.
 *
 * @param seq position in the log, assigned by the log and contiguous from 0.
 * @param type the event type, such as {@code turn/start}.
 * @param time wall-clock milliseconds at acceptance.
 * @param data the payload, already snapshotted and unwritable.
 */
public record SessionEvent(int seq, String type, long time, Object data) {}
