package io.akka.dsh.application;

import java.util.List;
import java.util.Map;

/** One loaded plugin: what state it is in, what it is still waiting for, and the way to
 * unload it. */
public final class PluginHandle {

  private final Plugin plugin;
  private final PluginRegistry registry;
  private PluginState state = PluginState.WAITING;
  private PluginScope scope;
  private Map<String, Object> present = Map.of();

  PluginHandle(Plugin plugin, PluginRegistry registry) {
    this.plugin = plugin;
    this.registry = registry;
  }

  public String name() {
    return plugin.name();
  }

  public PluginState state() {
    return state;
  }

  /** The declared services not currently present, named so a waiting plugin says what it
   * is waiting for. */
  public List<String> missing() {
    return plugin.injected().stream().filter(name -> !present.containsKey(name)).sorted().toList();
  }

  /** Unload the plugin: its registrations go, and its scope refuses new ones. */
  public void unload() {
    deactivate();
    state = PluginState.UNLOADED;
    registry.forget(plugin.name());
  }

  void reconsider(Map<String, Object> services) {
    if (state == PluginState.UNLOADED) {
      return;
    }
    present = services;
    var satisfied = plugin.injected().stream().allMatch(services::containsKey);
    if (satisfied && state == PluginState.WAITING) {
      // A fresh scope per activation: what the previous one registered was disposed with
      // it, and reusing it would let a second activation double the listeners.
      scope = PluginScope.forPlugin(plugin.injected(), Map.copyOf(services));
      state = PluginState.ACTIVE;
      plugin.body().accept(scope);
    } else if (!satisfied && state == PluginState.ACTIVE) {
      deactivate();
      state = PluginState.WAITING;
    }
  }

  private void deactivate() {
    if (scope != null) {
      scope.dispose();
      scope = null;
    }
  }
}
