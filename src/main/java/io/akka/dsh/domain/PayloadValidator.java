package io.akka.dsh.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Turns a caller's value into the value the log will hold: one recursive pass that
 * validates and copies at the same time.
 *
 * <p>Validation and copying are the same pass because a value that reports one thing when
 * checked and another when stored must not be expressible. A value whose accessor returns
 * something different on a second read would otherwise be validated in one shape and
 * stored in another.
 *
 * <p>The JSON serializer underneath refuses only self-reference. Non-finite numbers it
 * writes as strings, negative zero it preserves, and an arbitrary object it maps to
 * whatever its default is — so the rules below are held here rather than delegated.
 */
public final class PayloadValidator {

  private PayloadValidator() {}

  /**
   * @param value the caller's value.
   * @return a detached, unwritable copy holding only what JSON can spell.
   * @throws PayloadRefused with code {@code NOT_JSON} for anything else.
   */
  public static Object snapshot(Object value) {
    return copy(value, Collections.newSetFromMap(new IdentityHashMap<>()), "value");
  }

  private static Object copy(Object value, Set<Object> open, String path) {
    if (value == null || value instanceof String || value instanceof Boolean) {
      return value;
    }
    if (value instanceof Integer || value instanceof Long || value instanceof Short
        || value instanceof Byte) {
      return ((Number) value).longValue();
    }
    if (value instanceof Float || value instanceof Double) {
      return finite(((Number) value).doubleValue(), path);
    }
    if (value instanceof Map<?, ?> map) {
      return copyMap(map, open, path);
    }
    if (value instanceof List<?> list) {
      return copyList(list, open, path);
    }
    throw refuse(path, value.getClass().getName() + " has no JSON spelling");
  }

  private static Object finite(double number, String path) {
    if (!Double.isFinite(number)) {
      throw refuse(path, "a non-finite number has no JSON spelling");
    }
    // Negative zero survives a round trip through the serializer as a negative zero, so
    // nothing downstream would flag it; JSON itself cannot tell the two zeroes apart.
    if (number == 0.0d && Double.doubleToRawLongBits(number) != 0L) {
      throw refuse(path, "a negative zero has no JSON spelling distinct from zero");
    }
    return number;
  }

  private static Object copyMap(Map<?, ?> map, Set<Object> open, String path) {
    enter(open, map, path);
    try {
      var copied = new LinkedHashMap<String, Object>();
      for (var entry : map.entrySet()) {
        if (!(entry.getKey() instanceof String key)) {
          throw refuse(path, "a map key that is not a string has no JSON spelling");
        }
        copied.put(key, copy(entry.getValue(), open, path + "." + key));
      }
      return Collections.unmodifiableMap(copied);
    } finally {
      open.remove(map);
    }
  }

  private static Object copyList(List<?> list, Set<Object> open, String path) {
    enter(open, list, path);
    try {
      var copied = new ArrayList<>(list.size());
      for (var index = 0; index < list.size(); index++) {
        copied.add(copy(list.get(index), open, path + "[" + index + "]"));
      }
      return Collections.unmodifiableList(copied);
    } finally {
      open.remove(list);
    }
  }

  /** Tracks the containers on the current path by identity, so a value containing itself
   * is a refusal rather than a stack overflow. */
  private static void enter(Set<Object> open, Object container, String path) {
    if (!open.add(container)) {
      throw refuse(path, "a value that contains itself has no JSON spelling");
    }
  }

  private static PayloadRefused refuse(String path, String why) {
    return new PayloadRefused("NOT_JSON", "at " + path + ": " + why);
  }
}
