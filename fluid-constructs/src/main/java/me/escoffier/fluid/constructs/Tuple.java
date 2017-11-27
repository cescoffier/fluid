package me.escoffier.fluid.constructs;

import java.util.*;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Tuple implements Iterable<Object> {

  private List<Object> items;

  Tuple(Object... items) {
    if (items == null) {
      this.items = Collections.emptyList();
    } else {
      for (Object o : items) {
        if (o == null) {
          throw new IllegalArgumentException("A tuple cannot contain a `null` value");
        }
      }

      this.items = Collections.unmodifiableList(Arrays.asList(items));
    }
  }

  public static Tuple tuple(Object... items) {
    return new Tuple(items);
  }

  public int size() {
    return items.size();
  }


  @SuppressWarnings("unchecked")
  public <T> T getNth(int pos) {
    if (pos >= size()) {
      throw new IndexOutOfBoundsException(
        "Cannot retrieve item " + pos + " in tuple, size is " + size());
    }
    return (T) items.get(pos);
  }

  @Override
  public Iterator<Object> iterator() {
    return items.iterator();
  }

  public boolean contains(Object value) {
    for (Object val : this.items) {
      if (itemEquals(value, val)) {
        return true;
      }
    }
    return false;
  }

  public final boolean containsAll(Collection<?> collection) {
    for (final Object value : collection) {
      if (!contains(value)) {
        return false;
      }
    }
    return true;
  }


  public final boolean containsAll(final Object... values) {
    if (values == null) {
      throw new IllegalArgumentException("Values array cannot be null");
    }
    for (final Object value : values) {
      if (!contains(value)) {
        return false;
      }
    }
    return true;
  }


  public final int indexOf(Object value) {
    int i = 0;
    for (Object val : items) {
      if (itemEquals(value, val)) {
        return i;
      }
      i++;
    }
    return -1;
  }

  private boolean itemEquals(Object value, Object val) {
    // Values cannot be `null`
    return val.equals(value);
  }


  public final int lastIndexOf(Object value) {
    for (int i = size() - 1; i >= 0; i--) {
      final Object val = items.get(i);
      if (itemEquals(value, val)) {
        return i;
      }
    }
    return -1;
  }

  public final List<Object> asList() {
    return items;
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
      + ((this.items == null) ? 0 : this.items.hashCode());
    return result;
  }


  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Tuple other = (Tuple) obj;
    return this.items.equals(other.items);
  }
}
