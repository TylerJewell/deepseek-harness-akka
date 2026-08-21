package io.akka.dsh;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.dsh.api.SessionEndpoint;
import io.akka.dsh.api.SessionView;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 O1 — what a consumer misses across a dropped connection.
 *
 * <p>The source never had to answer this: its observers are in-process callbacks and there
 * is no connection between them and the log. This port has one, so it was given a rule, and
 * this is where the rule is checked. The disconnection is real rather than simulated by a
 * flag: the test simply stops asking, writes happen while it is not asking, and then it
 * resumes from the position it last saw.
 */
class SessionStreamIntegrationTest extends TestKitSupport {

  private void append(String id, String type, Map<String, Object> data) {
    httpClient
        .POST("/sessions/" + id + "/events")
        .withRequestBody(new SessionEndpoint.AppendRequest(type, data))
        .invoke();
  }

  private List<SessionView.EventView> since(String id, int afterSeq) {
    return httpClient
        .GET("/sessions/" + id + "/events/since/" + afterSeq)
        .responseBodyAsListOf(SessionView.EventView.class)
        .invoke()
        .body();
  }

  @Test
  void aConsumerResumingFromASeqIsServedEveryEventAfterIt() {
    var id = "stream-" + System.nanoTime();
    httpClient.POST("/sessions/" + id).invoke();

    append(id, "turn/start", Map.of("turn", 1));
    append(id, "turn/end", Map.of("turn", 1));

    // The consumer has read as far as seq 1 and then loses its connection.
    var firstRead = since(id, -1);
    assertThat(firstRead.stream().map(SessionView.EventView::seq)).containsExactly(0, 1);
    var lastSeen = firstRead.get(firstRead.size() - 1).seq();

    // Three writes land while nobody is reading.
    append(id, "turn/start", Map.of("turn", 2));
    append(id, "turn/end", Map.of("turn", 2));
    append(id, "turn/start", Map.of("turn", 3));

    // Resuming from the position it last saw serves all three, in order, none skipped and
    // none repeated.
    var resumed = since(id, lastSeen);
    assertThat(resumed.stream().map(SessionView.EventView::seq)).containsExactly(2, 3, 4);
    assertThat(resumed.stream().map(SessionView.EventView::type))
        .containsExactly("turn/start", "turn/end", "turn/start");
  }

  /** A consumer already up to date is served nothing rather than being served the log
   * again. */
  @Test
  void aConsumerThatIsUpToDateIsServedNothing() {
    var id = "caught-up-" + System.nanoTime();
    httpClient.POST("/sessions/" + id).invoke();
    append(id, "turn/start", Map.of("turn", 1));

    assertThat(since(id, 0)).isEmpty();
  }

  /** A consumer that never connected before asks from before the beginning and is served
   * the whole log — the same call, not a separate one. */
  @Test
  void aConsumerWithNoPositionYetIsServedTheWholeLog() {
    var id = "cold-" + System.nanoTime();
    httpClient.POST("/sessions/" + id).invoke();
    append(id, "turn/start", Map.of("turn", 1));
    append(id, "turn/end", Map.of("turn", 1));

    assertThat(since(id, -1).stream().map(SessionView.EventView::seq)).containsExactly(0, 1);
  }
}
