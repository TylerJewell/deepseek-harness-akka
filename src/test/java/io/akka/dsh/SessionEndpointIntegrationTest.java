package io.akka.dsh;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.dsh.api.SessionEndpoint;
import io.akka.dsh.api.SessionView;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The capability reached the way something outside a test reaches it. Every unit suite here
 * drives objects directly; this one drives the HTTP surface, because a port whose logic is
 * only ever called from its own tests has no reachable capability at all — and a
 * {@code graphical_surface: "none"} manifest answers a narrower question than that.
 */
class SessionEndpointIntegrationTest extends TestKitSupport {

  private String path(String suffix) {
    return "/sessions" + suffix;
  }

  private SessionView open(String id) {
    return httpClient.POST(path("/" + id)).responseBodyAs(SessionView.class).invoke().body();
  }

  private SessionView.EventView append(String id, String type, Map<String, Object> data) {
    return httpClient
        .POST(path("/" + id + "/events"))
        .withRequestBody(new SessionEndpoint.AppendRequest(type, data))
        .responseBodyAs(SessionView.EventView.class)
        .invoke()
        .body();
  }

  private SessionView get(String id) {
    return httpClient.GET(path("/" + id)).responseBodyAs(SessionView.class).invoke().body();
  }

  @Test
  void aSessionCanBeCreatedAppendedToForkedAndReadOverHttp() {
    var id = "http-" + System.nanoTime();
    open(id);

    assertThat(append(id, "turn/start", Map.of("turn", 1)).seq()).isZero();
    assertThat(append(id, "turn/end", Map.of("turn", 1)).seq()).isEqualTo(1);
    assertThat(append(id, "turn/start", Map.of("turn", 2)).seq()).isEqualTo(2);

    var read = get(id);
    assertThat(read.events().stream().map(SessionView.EventView::type))
        .containsExactly("turn/start", "turn/end", "turn/start");

    var childId = id + "-child";
    var child =
        httpClient
            .POST(path("/" + id + "/forks"))
            .withRequestBody(new SessionEndpoint.ForkRequest(childId, 1))
            .responseBodyAs(SessionView.class)
            .invoke()
            .body();

    assertThat(child.seedLength()).isEqualTo(2);
    assertThat(get(childId).events().stream().map(SessionView.EventView::type))
        .containsExactly("turn/start", "turn/end", "session/end-seed");
    assertThat(get(childId).parentSession()).isEqualTo(id);
  }

  /** A fork cutting into an open turn is refused over HTTP the same way it is in memory,
   * and the child it would have created does not exist. */
  @Test
  void aForkIntoAnOpenTurnIsRefusedOverHttp() {
    var id = "open-turn-" + System.nanoTime();
    open(id);
    append(id, "turn/start", Map.of("turn", 1));
    append(id, "turn/end", Map.of("turn", 1));
    append(id, "turn/start", Map.of("turn", 2));

    var refused =
        httpClient
            .POST(path("/" + id + "/forks"))
            .withRequestBody(new SessionEndpoint.ForkRequest(id + "-bad", 2))
            .invoke();
    assertThat(refused.status().isSuccess()).isFalse();

    var missingChild = httpClient.GET(path("/" + id + "-bad")).invoke();
    assertThat(missingChild.status().isSuccess()).isFalse();
  }

  /**
   * A boundary past the end is refused over HTTP.
   *
   * <p>The payload rule is deliberately not exercised here: a value with no JSON spelling
   * cannot be written in a JSON request body, so over this surface it is unreachable rather
   * than merely untested. Where it IS reachable — an in-process append — it is pinned by
   * {@code SessionLogTest#aRefusedAppendLeavesTheLogExactlyAsItWas}.
   */
  @Test
  void aBoundaryPastTheEndIsRefusedOverHttp() {
    var id = "bad-boundary-" + System.nanoTime();
    open(id);
    append(id, "turn/start", Map.of("turn", 1));

    var refused =
        httpClient
            .POST(path("/" + id + "/forks"))
            .withRequestBody(new SessionEndpoint.ForkRequest(id + "-nope", 99))
            .invoke();
    assertThat(refused.status().isSuccess()).isFalse();
    assertThat(get(id).seq()).isEqualTo(1);
  }
}
