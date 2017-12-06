package me.escoffier.fluid.constructs;

import java.util.*;

import static me.escoffier.fluid.constructs.impl.Windows.WINDOW_DATA;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FlowContext {

  private static final Stack<Map<String, Object>> context = new Stack<>();

  public synchronized static void pushContext(Map<String, Object> map) {
    context.push(map);
  }

  public synchronized static void popContext() {
    context.pop();
  }

  @SuppressWarnings("unchecked")
  public static synchronized <T> T get(String key) {
    Objects.requireNonNull(key);
    if (context.isEmpty()) {
      return null;
    } else {
      return (T) context.get(0).get(key);
    }
  }

  @SuppressWarnings("unchecked")
  public static synchronized <T> Optional<T> getOpt(String key) {
    Objects.requireNonNull(key);
    if (context.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.ofNullable((T) context.get(0).get(key));
    }
  }


  public static synchronized void set(String key, Object object) {
    Objects.requireNonNull(key);
    if (context.isEmpty()) {
      throw new IllegalStateException("No context to populate");
    } else {
      context.get(0).put(key, object);
    }
  }

  public static <T> List<Data<T>> window() {
    return get(WINDOW_DATA);
  }
}
