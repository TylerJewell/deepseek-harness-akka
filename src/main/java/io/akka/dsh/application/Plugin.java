package io.akka.dsh.application;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A unit of the harness: a name, the services it needs before it can run, and a body that
 * registers whatever it registers through the scope it is handed.
 */
public record Plugin(String name, Set<String> injected, Consumer<PluginScope> body) {

  /** Start describing a plugin. */
  public static Builder named(String name) {
    return new Builder(name);
  }

  /** Collects a plugin's declared dependencies before its body is known. */
  public static final class Builder {

    private final String name;
    private final Set<String> injected = new LinkedHashSet<>();

    private Builder(String name) {
      this.name = name;
    }

    /** Declare a service this plugin reads. Reading an undeclared one is refused. */
    public Builder inject(String... names) {
      injected.addAll(Set.of(names));
      return this;
    }

    public Plugin apply(Consumer<PluginScope> body) {
      return new Plugin(name, Set.copyOf(injected), body);
    }
  }
}
