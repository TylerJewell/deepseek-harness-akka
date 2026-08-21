package io.akka.dsh.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.akka.dsh.application.EventBus;
import io.akka.dsh.application.PluginScope;
import io.akka.dsh.application.SessionEvent;
import io.akka.dsh.application.SessionForkRefused;
import io.akka.dsh.application.SessionLog;
import io.akka.dsh.application.SessionStore;
import io.akka.dsh.domain.PayloadRefused;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The port side of the benchmark. Reads the same {@code bench/workloads.json} the source
 * runner reads and prints the same answer strings, so a difference in the output is a
 * difference in the two systems rather than in two descriptions of them.
 *
 * <p>Run: {@code mvn -q exec:java -Dexec.mainClass=io.akka.dsh.bench.BenchRunner
 * -Dexec.classpathScope=test -Dexec.args=<workloads.json>}
 */
public final class BenchRunner {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int WARMUP = 2_000;
  private static final int RUNS = 500;

  public static void main(String[] args) throws Exception {
    if ("--breakdown".equals(args[0])) {
      breakdown();
      return;
    }
    var workloads = MAPPER.readValue(Files.readString(Path.of(args[0])), List.class);
    var results = new LinkedHashMap<String, Map<String, Object>>();
    for (var raw : workloads) {
      @SuppressWarnings("unchecked")
      var workload = (Map<String, Object>) raw;
      var name = (String) workload.get("name");
      var answer = answer(workload);
      for (var n = 0; n < WARMUP; n++) {
        answer(workload);
      }
      var started = System.nanoTime();
      for (var n = 0; n < RUNS; n++) {
        answer(workload);
      }
      var elapsed = System.nanoTime() - started;
      results.put(name, Map.of("answer", answer, "nsPerOp", elapsed / RUNS));
    }
    System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(results));
  }

  /**
   * What fraction of the port's dispatch-bail number is building the bus rather than the
   * dispatching being compared — the same question asked of the source side, so the two
   * answers are about the same thing.
   */
  private static void breakdown() throws Exception {
    var construction = time(() -> {
      var unused = new EventBus();
      return unused;
    });
    var whole = time(() -> {
      var bus = new EventBus();
      var scope = PluginScope.root();
      bus.on(scope, "bench/event", a -> null);
      bus.on(scope, "bench/event", a -> false);
      bus.on(scope, "bench/event", a -> 0);
      bus.on(scope, "bench/event", a -> "late");
      bus.bail("bench/event");
      bus.onPrepend(scope, "bench/event", a -> "");
      return bus.bail("bench/event");
    });
    var prebuilt = new EventBus();
    var body = time(() -> {
      var scope = PluginScope.root();
      var registrations = new ArrayList<io.akka.dsh.application.Registration>();
      registrations.add(prebuilt.on(scope, "bench/event", a -> null));
      registrations.add(prebuilt.on(scope, "bench/event", a -> false));
      registrations.add(prebuilt.on(scope, "bench/event", a -> 0));
      registrations.add(prebuilt.on(scope, "bench/event", a -> "late"));
      prebuilt.bail("bench/event");
      registrations.add(prebuilt.onPrepend(scope, "bench/event", a -> ""));
      var result = prebuilt.bail("bench/event");
      registrations.forEach(io.akka.dsh.application.Registration::dispose);
      return result;
    });
    System.out.println(
        MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(
            new LinkedHashMap<>(Map.of(
                "constructionNs", construction,
                "wholeWorkloadNs", whole,
                "workloadWithoutConstructionNs", body,
                "constructionShare", Math.round(1000.0 * construction / whole) / 1000.0))));
  }

  private static long time(java.util.function.Supplier<Object> body) {
    for (var n = 0; n < 20_000; n++) {
      body.get();
    }
    var started = System.nanoTime();
    var runs = 20_000;
    for (var n = 0; n < runs; n++) {
      body.get();
    }
    return (System.nanoTime() - started) / runs;
  }

  @SuppressWarnings("unchecked")
  private static String answer(Map<String, Object> workload) {
    if ("arrival-orders".equals(workload.get("sequence"))) {
      return orderWorkload((List<Map<String, Object>>) workload.get("rows"));
    }
    var steps = (List<Map<String, Object>>) workload.get("steps");
    return ((String) workload.get("name")).startsWith("dispatch-")
        ? dispatchWorkload(steps)
        : sessionWorkload((String) workload.get("name"), steps);
  }

  /** The values a JSON workload cannot spell, built here so the file stays readable by
   * both runners. */
  private static Object badValue(String marker) {
    return switch (marker) {
      case "non-finite" -> Double.POSITIVE_INFINITY;
      case "negative-zero" -> -0.0d;
      case "exotic-object" -> new Date(0);
      case "self-referential" -> {
        var circular = new HashMap<String, Object>();
        circular.put("self", circular);
        yield circular;
      }
      default -> throw new IllegalArgumentException("unknown marker " + marker);
    };
  }

  private static Object payload(Map<String, Object> data) {
    var marker = data.get("__bad");
    return marker == null ? data : Map.of("value", badValue((String) marker));
  }

  private static String json(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof String text) {
      return "\"" + text + "\"";
    }
    return String.valueOf(value);
  }

  private static String dispatchWorkload(List<Map<String, Object>> steps) {
    var bus = new EventBus();
    var scope = PluginScope.root();
    var trace = new ArrayList<String>();
    var out = new ArrayList<String>();
    var n = new int[1];

    for (var step : steps) {
      var id = (String) step.get("id");
      var prepend = Boolean.TRUE.equals(step.get("prepend"));
      switch ((String) step.get("op")) {
        case "on" -> {
          var returns = step.get("returns");
          io.akka.dsh.application.Listener listener =
              a -> {
                trace.add(id);
                return returns;
              };
          if (prepend) {
            bus.onPrepend(scope, "bench/event", listener);
          } else {
            bus.on(scope, "bench/event", listener);
          }
        }
        case "onWaterfall" -> {
          var continues = Boolean.TRUE.equals(step.get("continues"));
          var returns = step.get("returns");
          io.akka.dsh.application.WaterfallListener composing =
              (a, next) -> {
                trace.add(id);
                if (!continues) {
                  return returns;
                }
                var result = next.get();
                trace.add(id + ":resumed");
                return result;
              };
          if (prepend) {
            bus.onWaterfallPrepend(scope, "bench/water", composing);
          } else {
            bus.onWaterfall(scope, "bench/water", composing);
          }
        }
        case "once" -> {
          var reenters = Boolean.TRUE.equals(step.get("reenters"));
          bus.once(
              scope,
              "bench/event",
              a -> {
                trace.add(id);
                if (reenters) {
                  bus.emit("bench/event");
                }
                return null;
              });
        }
        case "bail" -> {
          out.add("bail#" + (++n[0]) + "=" + json(bus.bail("bench/event")) + "|" + String.join(",", trace));
          trace.clear();
        }
        case "emit" -> {
          bus.emit("bench/event");
          out.add("emit#" + (++n[0]) + "=" + String.join(",", trace));
          trace.clear();
        }
        case "waterfall" -> {
          var result =
              bus.waterfall(
                  "bench/water",
                  () -> {
                    trace.add("builtin");
                    return "builtin";
                  });
          out.add("waterfall#" + (++n[0]) + "=" + json(result) + "|" + String.join(",", trace));
          trace.clear();
        }
        default -> throw new IllegalArgumentException("unknown op " + step.get("op"));
      }
    }
    return String.join(" ", out);
  }

  @SuppressWarnings("unchecked")
  private static String sessionWorkload(String name, List<Map<String, Object>> steps) {
    var store = new SessionStore(new EventBus());
    var out = new ArrayList<String>();
    var session = store.create(name + "-root");
    var forks = new int[1];
    var reopens = new int[1];

    for (var step : steps) {
      switch ((String) step.get("op")) {
        case "append" -> {
          var type = (String) step.get("type");
          try {
            var event = session.append(type, payload((Map<String, Object>) step.get("data")));
            out.add("ok:" + event.seq() + ":" + event.type());
          } catch (PayloadRefused refused) {
            out.add("refused:" + refused.code() + ":seq=" + session.seq());
          }
        }
        case "seed" -> {
          var types = (List<String>) step.get("types");
          var seed = new ArrayList<SessionEvent>();
          for (var index = 0; index < types.size(); index++) {
            seed.add(new SessionEvent(index, types.get(index), index + 1L, Map.of()));
          }
          session = SessionLog.seeded(name + "-seeded", seed);
          out.add("seeded:" + session.events().size() + ":" + session.firstLiveSeq());
        }
        case "reopen" -> {
          // Reopened from the session's OWN current log, which is what resuming a stored
          // session does — reopening a frozen earlier copy would never show the marker rule.
          session = SessionLog.seeded(name + "-reopen-" + (++reopens[0]), session.events());
          out.add("reopened:" + session.events().size() + ":" + session.firstLiveSeq());
        }
        case "fork" -> {
          var boundary = ((Number) step.get("boundary")).intValue();
          try {
            var child =
                store.fork(session, Optional.of(boundary), Optional.of(name + "-fork-" + (++forks[0])));
            out.add("fork@" + boundary + "=ok:" + child.events().size() + ":" + child.seedLength());
          } catch (SessionForkRefused refused) {
            out.add("fork@" + boundary + "=" + refused.code());
          }
        }
        default -> throw new IllegalArgumentException("unknown op " + step.get("op"));
      }
    }
    return String.join(" ", out);
  }

  private static String orderWorkload(List<Map<String, Object>> rows) {
    var answers = new ArrayList<String>();
    for (var order : permutations(rows)) {
      var bus = new EventBus();
      var scope = PluginScope.root();
      var names = new ArrayList<String>();
      for (var row : order) {
        var returns = row.get("returns");
        names.add((String) row.get("id"));
        io.akka.dsh.application.Listener listener = a -> returns;
        if (Boolean.TRUE.equals(row.get("prepend"))) {
          bus.onPrepend(scope, "bench/order", listener);
        } else {
          bus.on(scope, "bench/order", listener);
        }
      }
      answers.add(String.join(">", names) + "=" + json(bus.bail("bench/order")));
    }
    return String.join(" ", answers);
  }

  private static <T> List<List<T>> permutations(List<T> items) {
    if (items.size() <= 1) {
      return List.of(new ArrayList<>(items));
    }
    var out = new ArrayList<List<T>>();
    for (var index = 0; index < items.size(); index++) {
      var rest = new ArrayList<>(items);
      var head = rest.remove(index);
      for (var tail : permutations(rest)) {
        var permutation = new ArrayList<T>();
        permutation.add(head);
        permutation.addAll(tail);
        out.add(permutation);
      }
    }
    return out;
  }

  private BenchRunner() {}
}
