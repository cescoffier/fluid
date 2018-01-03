package me.escoffier.fluid.constructs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Represents an item transiting on a stream.
 * Instances of {@link Data} are immutable.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Data<T> {

  private static final Object STUB = new Object();
  /**
   * The payload, must not be {@code null}
   */
  private final T payload;

  /**
   * A set of headers.
   */
  private final Map<String, Object> headers;

  /**
   * Creates a new instance of {@link Data}. This constructor does not set any headers (empty map).
   *
   * @param payload the payload, must not be {@code null}
   */
  public Data(T payload) {
    this.payload = Objects.requireNonNull(payload);
    this.headers = Collections.emptyMap();
  }

  /**
   * Creates a new instance of {@link Data}. This constructor does not set any headers (empty map).
   *
   * @param payload the payload
   * @param headers the header, must not be {@code null}
   */
  public Data(T payload, Map<String, Object> headers) {
    this.payload = Objects.requireNonNull(payload);
    this.headers = Collections
      .unmodifiableMap(new HashMap<>(Objects.requireNonNull(headers)));
  }

  protected Data() {
    this((T) STUB, Collections.emptyMap());
  }

  /**
   * Creates a new instance of {@link Data} reusing the same set of headers but with the new payload.
   *
   * @param payload the payload
   * @param <O>  the type of the encapsulated payload
   * @return the new instance
   */
  public <O> Data<O> with(O payload) {
    return new Data<>(Objects.requireNonNull(payload), headers());
  }

  /**
   * Retrieves the value associated with the passed key in the headers.
   *
   * @param key the key, must not be {@code null}
   * @param <X> the expected type
   * @return the value, {@code null} if not found
   */
  @SuppressWarnings("unchecked")
  public <X> X get(String key) {
    return (X) headers.get(Objects.requireNonNull(key));
  }

  /**
   * Same as {@link #get(String)} but returns an {@link Optional} as result.
   *
   * @param key the key, must not be {@code null}
   * @param <X> the expected type
   * @return an {@link Optional} containing the value if present
   */
  @SuppressWarnings("unchecked")
  public <X> Optional<X> getOpt(String key) {
    return Optional.ofNullable((X) headers.get(Objects.requireNonNull(key)));
  }

  /**
   * Creates a new instance of {@link Data} copying the current instance and adding the given header:
   * <code>key: value</code>.
   *
   * @param key   the key of the header to add, must not be {@code null}
   * @param value the value to associate to the key
   * @return the new instance of {@link Data}
   */
  public Data<T> with(String key, Object value) {
    Map<String, Object> copy = new HashMap<>(headers);
    copy.put(Objects.requireNonNull(key), value);
    return new Data<>(payload, copy);
  }

  /**
   * Creates a new instance of {@link Data} copying the current instance without the given header. This method does
   * not check whether or not the header was present.
   *
   * @param key the key of the header to remove, must not be {@code null}
   * @return the new instance of {@link Data}
   */
  public Data<T> without(String key) {
    Map<String, Object> copy = new HashMap<>(headers);
    copy.remove(Objects.requireNonNull(key));
    return new Data<>(payload, copy);
  }

  /**
   * Gets the set of headers associated with the current {@link Data}.
   *
   * @return the headers, never {@code null}. An empty map is returned if the {@link Data} has no headers.
   */
  public Map<String, Object> headers() {
    return headers;
  }

  /**
   * @return the encapsulated payload, never {@code null}
   */
  public T payload() {
    return payload;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{\"payload\":\"" + payload.toString() + "\", \"headers\":");
    if (! headers.isEmpty()) {
      builder.append("{");
      String h = headers.entrySet().stream()
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

  public boolean isControl() {
    return ControlData.isControl(this);
  }
}
