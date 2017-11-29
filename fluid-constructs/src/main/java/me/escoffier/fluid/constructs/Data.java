package me.escoffier.fluid.constructs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represent an item transiting on a stream.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Data<T> {

  private final T data;

  private final Map<String, Object> headers;

  public Data(T content) {
    this.data = Objects.requireNonNull(content);
    this.headers = Collections.emptyMap();
  }

  public Data(T content, Map<String, Object> headers) {
    this.data = Objects.requireNonNull(content);
    this.headers = Collections
      .unmodifiableMap(new HashMap<>(Objects.requireNonNull(headers)));
  }

  public <O> Data<O> with(O data) {
    return new Data<>(Objects.requireNonNull(data), headers());
  }

  @SuppressWarnings("unchecked")
  public <X> X get(String key) {
    return (X) headers.get(Objects.requireNonNull(key));
  }

  public Data<T> with(String key, Object value) {
    Map<String, Object> copy = new HashMap<>(headers);
    copy.put(Objects.requireNonNull(key), value);
    return new Data<>(data, copy);
  }

  public Data<T> without(String key) {
    Map<String, Object> copy = new HashMap<>(headers);
    copy.remove(Objects.requireNonNull(key));
    return new Data<>(data, copy);
  }

  public Map<String, Object> headers() {
    return headers;
  }
  public T item() {
    return data;
  }

}
