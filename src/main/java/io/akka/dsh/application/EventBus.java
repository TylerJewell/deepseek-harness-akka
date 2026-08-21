package io.akka.dsh.application;

import io.akka.dsh.domain.BailRule;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * The five ways one event reaches its listeners.
 *
 * <p>{@code emit} hands the event over and returns. {@code parallel} waits for all of
 * them. {@code bail} and {@code serial} stop at the first listener that answers.
 * {@code waterfall} composes listeners around the built-in behaviour so any of them can
 * veto it.
 *
 * <p>A listener's lifetime belongs to the {@link PluginScope} that registered it.
 */
public final class EventBus {

  private final Map<String, List<Hook>> hooks = new LinkedHashMap<>();

  private record Hook(PluginScope scope, Listener listener, WaterfallListener waterfall) {}

  /** Register a listener, after those already registered for this event. */
  public Registration on(PluginScope scope, String name, Listener listener) {
    return register(scope, name, new Hook(scope, listener, null), false);
  }

  /** Register a listener ahead of every listener already registered for this event. */
  public Registration onPrepend(PluginScope scope, String name, Listener listener) {
    return register(scope, name, new Hook(scope, listener, null), true);
  }

  /** Register a listener in the composing mode; see {@link #waterfall}. */
  public Registration onWaterfall(PluginScope scope, String name, WaterfallListener listener) {
    return register(scope, name, new Hook(scope, null, listener), false);
  }

  /**
   * Register a composing listener ahead of every listener already registered for this
   * event. Placement is one rule across all five modes, and it is sharper here than
   * elsewhere: the outermost listener is the one that can veto the rest, so which listener
   * is first decides what the others get to see at all.
   */
  public Registration onWaterfallPrepend(
      PluginScope scope, String name, WaterfallListener listener) {
    return register(scope, name, new Hook(scope, null, listener), true);
  }

  /**
   * Register a listener removed before its body runs, so a dispatch made from inside it
   * does not reach it a second time.
   */
  public Registration once(PluginScope scope, String name, Listener listener) {
    var holder = new Registration[1];
    holder[0] =
        on(
            scope,
            name,
            args -> {
              holder[0].dispose();
              return listener.call(args);
            });
    return holder[0];
  }

  private Registration register(PluginScope scope, String name, Hook hook, boolean prepend) {
    scope.assertActive();
    var list = hooks.computeIfAbsent(name, key -> new ArrayList<>());
    if (prepend) {
      list.add(0, hook);
    } else {
      list.add(hook);
    }
    Registration registration = () -> list.remove(hook);
    scope.own(registration);
    return registration;
  }

  /** A snapshot, so a listener registering or disposing during dispatch does not change
   * who this dispatch reaches. */
  private List<Hook> listeners(String name) {
    return List.copyOf(hooks.getOrDefault(name, List.of()));
  }

  /** The listeners registered for one event, in the order a dispatch would reach them. */
  public List<Listener> listenersOf(String name) {
    return listeners(name).stream().map(Hook::listener).toList();
  }

  /** Call every listener and return. A listener that finishes later finishes later. */
  public void emit(String name, Object... args) {
    for (var hook : listeners(name)) {
      hook.listener().call(args);
    }
  }

  /**
   * Call every listener and wait for all of them, whatever any of them does. Where more
   * than one failed, every failure is attached to the raised {@link DispatchFailed}.
   */
  public void parallel(String name, Object... args) {
    var running = new ArrayList<CompletableFuture<Throwable>>();
    for (var hook : listeners(name)) {
      running.add(
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  hook.listener().call(args);
                  return null;
                } catch (RuntimeException failure) {
                  return (Throwable) failure;
                }
              }));
    }
    CompletableFuture.allOf(running.toArray(CompletableFuture[]::new)).join();

    var failures = running.stream().map(CompletableFuture::join).filter(f -> f != null).toList();
    if (failures.isEmpty()) {
      return;
    }
    var aggregate = new DispatchFailed(failures.size() + " listener(s) of \"" + name + "\" failed");
    failures.forEach(aggregate::addSuppressed);
    throw aggregate;
  }

  /** Call listeners in order until one answers; return that answer, or nothing. */
  public Object bail(String name, Object... args) {
    for (var hook : listeners(name)) {
      var result = hook.listener().call(args);
      if (BailRule.stops(result)) {
        return result;
      }
    }
    return null;
  }

  /**
   * The same rule as {@link #bail}, waiting for each listener before moving to the next.
   * On this runtime a listener is an ordinary call, so waiting for it is running it — the
   * mode is kept separate because the stop point is what a caller depends on, and a
   * listener that becomes asynchronous later must not silently change it.
   */
  public Object serial(String name, Object... args) {
    return bail(name, args);
  }

  /**
   * Compose listeners around {@code builtIn}. Listeners run outermost-first; one that does
   * not call its continuation prevents the remaining listeners and the built-in behaviour
   * from running at all.
   *
   * @return the outermost listener's result, or the built-in one's when there are none.
   */
  public Object waterfall(String name, Supplier<Object> builtIn, Object... args) {
    var remaining = new ArrayDeque<>(listeners(name));
    var chain =
        new Supplier<Object>() {
          @Override
          public Object get() {
            var hook = remaining.poll();
            return hook == null ? builtIn.get() : hook.waterfall().call(args, this);
          }
        };
    return chain.get();
  }
}
