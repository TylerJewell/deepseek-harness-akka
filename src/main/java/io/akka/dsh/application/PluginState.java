package io.akka.dsh.application;

/** Where a loaded plugin has got to. */
public enum PluginState {
  /** Declared dependencies are not all present, so the plugin body has not run. */
  WAITING,
  /** The body has run and its registrations are live. */
  ACTIVE,
  /** Unloaded; its registrations are gone and its scope refuses new ones. */
  UNLOADED
}
