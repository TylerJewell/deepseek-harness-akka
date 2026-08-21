package io.akka.dsh.api;

import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpException;
import io.akka.dsh.application.ForkCode;
import io.akka.dsh.application.SessionEntity;
import io.akka.dsh.application.SessionForkRefused;
import io.akka.dsh.domain.SessionRecord;
import java.util.List;
import java.util.Map;

/**
 * The way in from outside. Everything the port rebuilds is reachable here: opening a
 * session, appending to its log, reading the log, resuming a read from a position, and
 * forking a child from a stable prefix.
 *
 * <p>Open to any caller because that is what makes the capability reachable at all; the
 * surface holds no credentials and every write is validated by the entity behind it.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/sessions")
public class SessionEndpoint {

  private final ComponentClient componentClient;

  public SessionEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  public record AppendRequest(String type, Map<String, Object> data) {}

  public record ForkRequest(String childId, Integer boundary) {}

  /** Open a session with no replayed history. */
  @Post("/{sessionId}")
  public SessionView open(String sessionId) {
    return SessionView.of(
        componentClient
            .forEventSourcedEntity(sessionId)
            .method(SessionEntity::open)
            .invoke(new SessionEntity.Open(List.of(), null)));
  }

  /** Append one event. */
  @Post("/{sessionId}/events")
  public SessionView.EventView append(String sessionId, AppendRequest body) {
    return SessionView.EventView.of(
        componentClient
            .forEventSourcedEntity(sessionId)
            .method(SessionEntity::append)
            .invoke(new SessionEntity.Append(body.type(), body.data())));
  }

  /** The whole durable log. */
  @Get("/{sessionId}")
  public SessionView get(String sessionId) {
    return SessionView.of(
        componentClient.forEventSourcedEntity(sessionId).method(SessionEntity::get).invoke());
  }

  /**
   * Everything after {@code afterSeq}.
   *
   * <p>The source publishes accepted events to in-process observers, where there is no
   * connection to lose and so nothing to say about losing one. Here there is: a consumer
   * names the last position it saw and is served the rest from the durable log rather than
   * from a buffer of recent dispatches, so a consumer that was away misses nothing however
   * long it was away.
   */
  @Get("/{sessionId}/events/since/{afterSeq}")
  public List<SessionView.EventView> since(String sessionId, int afterSeq) {
    return SessionView.of(
        componentClient
            .forEventSourcedEntity(sessionId)
            .method(SessionEntity::since)
            .invoke(afterSeq));
  }

  /**
   * Fork a child from a stable prefix. The boundary is inclusive; omitted, it is the
   * source's last event. A prefix ending inside an open turn is refused.
   *
   * <p>A refusal is a bad request with its code in the body, not an unhandled failure: the
   * five ways a fork can be wrong are the caller's to tell apart, which is the whole reason
   * they are separate codes.
   */
  @Post("/{sessionId}/forks")
  public SessionView fork(String sessionId, ForkRequest body) {
    var source =
        componentClient.forEventSourcedEntity(sessionId).method(SessionEntity::get).invoke();
    List<SessionRecord.StoredEvent> prefix;
    try {
      prefix = prefixOf(sessionId, source, body.boundary());
    } catch (SessionForkRefused refused) {
      throw HttpException.badRequest(refused.code() + ": " + refused.getMessage());
    }
    var childId = body.childId() == null ? sessionId + "-fork-" + prefix.size() : body.childId();
    return SessionView.of(
        componentClient
            .forEventSourcedEntity(childId)
            .method(SessionEntity::open)
            .invoke(new SessionEntity.Open(prefix, sessionId)));
  }

  /**
   * The same prefix rule the in-memory store applies, over the durable record. Only turn
   * markers are read, so a boundary landing on the end-of-seed marker looks past it at
   * whichever turn marker came before.
   */
  private static List<SessionRecord.StoredEvent> prefixOf(
      String sessionId, SessionRecord source, Integer boundary) {
    var events = source.events();
    if (boundary == null) {
      return List.copyOf(events);
    }
    if (boundary < 0 || boundary >= events.size()) {
      throw new SessionForkRefused(
          ForkCode.INVALID_BOUNDARY,
          "boundary " + boundary + " does not exist in session \"" + sessionId + "\"");
    }
    var prefix = List.copyOf(events.subList(0, boundary + 1));
    for (var index = prefix.size() - 1; index >= 0; index--) {
      var type = prefix.get(index).type();
      if ("turn/end".equals(type)) {
        break;
      }
      if ("turn/start".equals(type)) {
        throw new SessionForkRefused(
            ForkCode.OPEN_TURN,
            "boundary " + boundary + " in session \"" + sessionId + "\" ends inside an open turn");
      }
    }
    return prefix;
  }
}
