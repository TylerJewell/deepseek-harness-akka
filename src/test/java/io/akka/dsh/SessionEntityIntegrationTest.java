package io.akka.dsh;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.dsh.application.SessionEntity;
import io.akka.dsh.domain.SessionRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The durable half, against a running runtime. The unit suites pin the rules; this pins
 * that they survive being written to a journal and read back from it — which is the one
 * thing an in-memory suite structurally cannot check.
 */
class SessionEntityIntegrationTest extends TestKitSupport {

  private SessionRecord open(String id) {
    return componentClient
        .forEventSourcedEntity(id)
        .method(SessionEntity::open)
        .invoke(new SessionEntity.Open(List.of(), null));
  }

  private SessionRecord.StoredEvent append(String id, String type, Map<String, Object> data) {
    return componentClient
        .forEventSourcedEntity(id)
        .method(SessionEntity::append)
        .invoke(new SessionEntity.Append(type, data));
  }

  private SessionRecord read(String id) {
    return componentClient.forEventSourcedEntity(id).method(SessionEntity::get).invoke();
  }

  /** The log replays to the same sequence it was written in, with the same payloads. */
  @Test
  void aReloadedSessionReplaysTheSameLog() {
    var id = "reload-" + System.nanoTime();
    open(id);
    append(id, "turn/start", Map.of("turn", 1));
    append(id, "turn/end", Map.of("turn", 1));

    var record = read(id);
    assertThat(record.events().stream().map(SessionRecord.StoredEvent::seq)).containsExactly(0, 1);
    assertThat(record.events().stream().map(SessionRecord.StoredEvent::type))
        .containsExactly("turn/start", "turn/end");
    assertThat(record.seq()).isEqualTo(2);
  }

  /**
   * Where the payload rule can and cannot act.
   *
   * <p>A command reaching an entity has already been serialized, and the serializer turns a
   * non-finite number into the string "NaN" without raising. So by the time the entity's
   * validator sees the value it is a string, and a string is a perfectly good JSON value —
   * the refusal cannot happen here, and asserting that it does would be asserting about a
   * value the entity never receives.
   *
   * <p>What the entity does guarantee is that the value stored is the value that arrived.
   * The refusal belongs at the in-process append site, where the value is still a number;
   * {@code SessionLogTest#aRefusedAppendLeavesTheLogExactlyAsItWas} pins it there. This is
   * listed in the README as a difference from the original, whose observers are in-process
   * and so never cross a serializer at all.
   */
  @Test
  void aNonFiniteNumberHasAlreadyBecomeAStringByTheTimeTheEntitySeesIt() {
    var id = "serialized-" + System.nanoTime();
    open(id);
    var stored = append(id, "turn/start", Map.of("value", Double.NaN));
    assertThat(stored.data()).isEqualTo(Map.of("value", "NaN"));
    assertThat(read(id).seq()).isEqualTo(1);
  }

  /** Opening the same session twice is refused. */
  @Test
  void openingAnExistingSessionIsRefused() {
    var id = "twice-" + System.nanoTime();
    open(id);
    assertThatThrownBy(() -> open(id)).hasMessageContaining("already exists");
  }

  /** A session opened from replayed history gains one end-of-seed marker; one that already
   * ends in a marker does not gain a second. */
  @Test
  void aSeededSessionIsMarkedOnceHoweverManyTimesItIsReopened() {
    var first = "seeded-" + System.nanoTime();
    componentClient
        .forEventSourcedEntity(first)
        .method(SessionEntity::open)
        .invoke(
            new SessionEntity.Open(
                List.of(
                    new SessionRecord.StoredEvent(0, "turn/start", 1L, Map.of()),
                    new SessionRecord.StoredEvent(1, "turn/end", 2L, Map.of())),
                null));
    var seeded = read(first);
    assertThat(seeded.events().stream().map(SessionRecord.StoredEvent::type))
        .containsExactly("turn/start", "turn/end", "session/end-seed");
    assertThat(seeded.seedLength()).isEqualTo(2);

    var second = "reseeded-" + System.nanoTime();
    componentClient
        .forEventSourcedEntity(second)
        .method(SessionEntity::open)
        .invoke(new SessionEntity.Open(seeded.events(), first));
    assertThat(read(second).events().stream().map(SessionRecord.StoredEvent::type))
        .containsExactly("turn/start", "turn/end", "session/end-seed");
  }

  /** A seed that is not contiguous from zero is refused, the same as a live append with a
   * bad payload. */
  @Test
  void aSeedThatIsNotContiguousIsRefused() {
    var id = "gap-" + System.nanoTime();
    assertThatThrownBy(
            () ->
                componentClient
                    .forEventSourcedEntity(id)
                    .method(SessionEntity::open)
                    .invoke(
                        new SessionEntity.Open(
                            List.of(new SessionRecord.StoredEvent(3, "turn/start", 1L, Map.of())),
                            null)))
        .hasMessageContaining("SEED_NOT_CONTIGUOUS");
  }
}
