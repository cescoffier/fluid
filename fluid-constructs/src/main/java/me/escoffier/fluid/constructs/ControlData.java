package me.escoffier.fluid.constructs;

import java.util.stream.Collectors;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ControlData<T> extends Data<T> {

  public ControlData() {
    super();
  }

  public String toString(String type) {
    StringBuilder builder = new StringBuilder("{\"control\":\"" + type + "\", \"headers\":");
    if (! headers().isEmpty()) {
      builder.append("{");
      String h = headers().entrySet().stream()
        .map(entry -> "\"" + entry.getKey() + "\":\""
          + (entry.getValue() != null ? (entry.getValue() + "\"") : "NULL")
        )
        .collect(Collectors.joining(","));
      builder.append(h).append("}");
    } else {
      builder.append("{}");
    }
    builder.append("}");
    return builder.toString();
  }

  @Override
  public String toString() {
    return toString("control");
  }

  @Override
  public <X> X get(String key) {
    return (X) headers().get(key);
  }

  public static boolean isControl(Object d) {
    return d instanceof ControlData;
  }

  public static boolean isNotControl(Object d) {
    return !(d instanceof ControlData);
  }
}
