package io.akka.dsh.application;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds the loaded plugins and the services they read.
 *
 * <p>A plugin runs when every service it declared is present, and stops running when one
 * of them is withdrawn. Each activation gets a fresh scope, so a plugin that comes back
 * does not carry the previous activation's registrations with it.
 */
public final class PluginRegistry {

  private final EventBus bus;
  private final Map<String, Object> services = new LinkedHashMap<>();
  private final Map<String, PluginHandle> loaded = new LinkedHashMap<>();

  public PluginRegistry(EventBus bus) {
    this.bus = bus;
  }

  /**
   * Load a plugin, activating it now if its declared services are already present.
   *
   * @throws IllegalStateException if a plugin of this name is already loaded.
   */
  public PluginHandle load(Plugin plugin) {
    if (loaded.containsKey(plugin.name())) {
      throw new IllegalStateException("plugin \"" + plugin.name() + "\" is already loaded");
    }
    var handle = new PluginHandle(plugin, this);
    loaded.put(plugin.name(), handle);
    handle.reconsider(services);
    return handle;
  }

  /** Make a service available, activating whatever was waiting for it. */
  public void provide(String name, Object service) {
    services.put(name, service);
    for (var handle : List.copyOf(loaded.values())) {
      handle.reconsider(services);
    }
  }

  /** Take a service away, unloading whatever depends on it back to waiting. */
  public void withdraw(String name) {
    services.remove(name);
    for (var handle : List.copyOf(loaded.values())) {
      handle.reconsider(services);
    }
  }

  /** Loaded plugins, in load order. */
  public List<PluginHandle> list() {
    return new ArrayList<>(loaded.values());
  }

  EventBus bus() {
    return bus;
  }

  void forget(String name) {
    loaded.remove(name);
  }
}
