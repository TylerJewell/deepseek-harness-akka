package io.akka.dsh.application;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What owns a plugin's registrations. Disposing the scope is the only thing that removes
 * them; a plugin never unregisters its own listeners.
 *
 * <p>A scope also decides which services the plugin may read: only the ones it declared.
 * An undeclared read is refused with the name in the message, because the alternative — a
 * plugin whose body simply does not run, with nothing said about why — is the failure mode
 * that cost this port's own probe an investigation.
 */
public final class PluginScope {

  private final Set<String> declared;
  private final Map<String, Object> services;
  private final List<Registration> owned = new ArrayList<>();
  private boolean active = true;

  private PluginScope(Set<String> declared, Map<String, Object> services) {
    this.declared = declared;
    this.services = services;
  }

  /** A scope with no plugin above it, for listeners the application itself owns. */
  public static PluginScope root() {
    return new PluginScope(Set.of(), Map.of());
  }

  static PluginScope forPlugin(Set<String> declared, Map<String, Object> services) {
    return new PluginScope(declared, services);
  }

  /**
   * @param name a service this plugin declared.
   * @return the provided service.
   * @throws IllegalStateException if the plugin did not declare it.
   */
  public Object service(String name) {
    if (!declared.contains(name)) {
      throw new IllegalStateException(
          "UNDECLARED_DEPENDENCY: this plugin did not declare \"" + name + "\"");
    }
    return services.get(name);
  }

  /** @throws IllegalStateException if the scope has been disposed. */
  void assertActive() {
    if (!active) {
      throw new IllegalStateException("INACTIVE_SCOPE: this plugin has been unloaded");
    }
  }

  void own(Registration registration) {
    owned.add(registration);
  }

  void dispose() {
    if (!active) {
      return;
    }
    active = false;
    for (var registration : List.copyOf(owned)) {
      registration.dispose();
    }
    owned.clear();
  }
}
