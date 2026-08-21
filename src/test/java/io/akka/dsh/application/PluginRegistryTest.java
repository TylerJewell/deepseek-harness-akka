package io.akka.dsh.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 D9, D10 and the dependency gate. */
class PluginRegistryTest {

  /** D9 — the plugin unregisters nothing itself; unloading it is what removes its work. */
  @Test
  void unloadingAPluginRemovesEveryListenerItRegistered() {
    var bus = new EventBus();
    var registry = new PluginRegistry(bus);
    var seen = new ArrayList<String>();

    var handle =
        registry.load(
            Plugin.named("watcher")
                .apply(scope -> {
                  bus.on(scope, "probe", args -> {
                    seen.add("one");
                    return null;
                  });
                  bus.on(scope, "probe", args -> {
                    seen.add("two");
                    return null;
                  });
                }));

    bus.emit("probe");
    assertEquals(List.of("one", "two"), seen);

    handle.unload();
    bus.emit("probe");
    assertEquals(List.of("one", "two"), seen);
  }

  /** D10 — a scope that has been unloaded refuses a registration rather than accepting
   * one nothing owns. */
  @Test
  void registeringFromAnUnloadedScopeIsRefusedAtTheCall() {
    var bus = new EventBus();
    var registry = new PluginRegistry(bus);
    var captured = new PluginScope[1];

    var handle = registry.load(Plugin.named("captures").apply(scope -> captured[0] = scope));
    handle.unload();

    var refused =
        assertThrows(
            IllegalStateException.class, () -> bus.on(captured[0], "probe", args -> null));
    assertTrue(refused.getMessage().contains("INACTIVE_SCOPE"));
  }

  /**
   * The dependency gate. In the source, a plugin that reads a service it never declared
   * simply does not run and says nothing about why — the failure is silent enough that it
   * cost a probe of this port an investigation. Here a declared dependency is the only way
   * to reach a service, and an undeclared read is refused with the name in the message.
   */
  @Test
  void aPluginWaitsForTheServicesItDeclaresAndActivatesWhenTheyArrive() {
    var bus = new EventBus();
    var registry = new PluginRegistry(bus);
    var ran = new ArrayList<String>();

    var handle =
        registry.load(
            Plugin.named("needs-sessions")
                .inject("sessions")
                .apply(scope -> ran.add("activated with " + scope.service("sessions"))));

    assertEquals(List.of(), ran);
    assertEquals(PluginState.WAITING, handle.state());
    assertEquals(List.of("sessions"), handle.missing());

    registry.provide("sessions", "the-store");

    assertEquals(List.of("activated with the-store"), ran);
    assertEquals(PluginState.ACTIVE, handle.state());
  }

  /** An undeclared service read is refused, and the message names what was not declared. */
  @Test
  void readingAServiceThePluginDidNotDeclareIsRefusedWithTheNameInTheMessage() {
    var bus = new EventBus();
    var registry = new PluginRegistry(bus);
    registry.provide("sessions", "the-store");

    var failure = new IllegalStateException[1];
    registry.load(
        Plugin.named("undeclared")
            .apply(scope -> failure[0] = assertThrows(IllegalStateException.class, () -> scope.service("sessions"))));

    assertTrue(failure[0].getMessage().contains("sessions"));
    assertTrue(failure[0].getMessage().contains("UNDECLARED_DEPENDENCY"));
  }

  /** Withdrawing a service unloads the plugins that depend on it, and they come back
   * when it returns. */
  @Test
  void withdrawingAServiceUnloadsItsDependentsAndProvidingItAgainRestoresThem() {
    var bus = new EventBus();
    var registry = new PluginRegistry(bus);
    var lifecycle = new ArrayList<String>();

    registry.provide("sessions", "v1");
    var handle =
        registry.load(
            Plugin.named("dependent")
                .inject("sessions")
                .apply(scope -> lifecycle.add("up:" + scope.service("sessions"))));
    assertEquals(List.of("up:v1"), lifecycle);

    registry.withdraw("sessions");
    assertEquals(PluginState.WAITING, handle.state());

    registry.provide("sessions", "v2");
    assertEquals(List.of("up:v1", "up:v2"), lifecycle);
  }

  /** A plugin activated a second time after a withdrawal does not keep the listeners it
   * registered the first time. */
  @Test
  void aReactivatedPluginDoesNotKeepTheListenersFromItsPreviousActivation() {
    var bus = new EventBus();
    var registry = new PluginRegistry(bus);
    var seen = new ArrayList<String>();

    registry.provide("sessions", "v1");
    registry.load(
        Plugin.named("dependent")
            .inject("sessions")
            .apply(scope -> bus.on(scope, "probe", args -> {
              seen.add("hit");
              return null;
            })));

    bus.emit("probe");
    registry.withdraw("sessions");
    registry.provide("sessions", "v2");
    bus.emit("probe");

    assertEquals(List.of("hit", "hit"), seen);
  }

  /** Two plugins with the same name cannot both be loaded. */
  @Test
  void aDuplicatePluginNameIsRefused() {
    var registry = new PluginRegistry(new EventBus());
    registry.load(Plugin.named("dup").apply(scope -> {}));
    assertThrows(IllegalStateException.class, () -> registry.load(Plugin.named("dup").apply(scope -> {})));
  }

  /** Loaded plugins are reported in load order, with their state. */
  @Test
  void loadedPluginsAreReportedInLoadOrder() {
    var registry = new PluginRegistry(new EventBus());
    registry.load(Plugin.named("a").apply(scope -> {}));
    registry.load(Plugin.named("b").inject("absent").apply(scope -> {}));
    assertEquals(List.of("a", "b"), registry.list().stream().map(PluginHandle::name).toList());
    assertEquals(
        List.of(PluginState.ACTIVE, PluginState.WAITING),
        registry.list().stream().map(PluginHandle::state).toList());
  }
}
