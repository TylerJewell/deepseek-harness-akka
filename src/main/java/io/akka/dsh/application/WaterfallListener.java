package io.akka.dsh.application;

import java.util.function.Supplier;

/**
 * A listener that wraps the rest of the chain. Calling {@code next} runs the next
 * listener, and finally the built-in behaviour; not calling it vetoes both.
 */
@FunctionalInterface
public interface WaterfallListener {
  Object call(Object[] args, Supplier<Object> next);
}
