package me.escoffier.fluid.constructs;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a pair.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Pair<L, R> extends Tuple implements Map.Entry<L, R> {

  private final L left;
  private final R right;

  private Pair(L left, R right) {
    super(left, right);
    this.left = left;
    this.right = right;
  }

  public static <L, R> Pair<L, R> pair(L l, R r) {
    return new Pair<>(Objects.requireNonNull(l, "Left cannot be `null'"),
      Objects.requireNonNull(r, "Right cannot be `null`"));
  }

  public Pair<L, R> setLeft(L l) {
    return pair(l, right);
  }

  public Pair<L, R> setRight(R r) {
    return pair(left, r);
  }

  public L left() {
    return left;
  }

  public R right() {
    return right;
  }

  @Override
  public L getKey() {
    return left;
  }

  @Override
  public R getValue() {
    return right;
  }

  @Override
  public R setValue(R value) {
    throw new UnsupportedOperationException("Pairs are immutable, use `setRight`");
  }

  /**
   * Returns a suitable hash code for Pair.
   * The hash code follows the definition in {@code Map.Entry}.
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    // see Map.Entry API specification
    return (getKey() == null ? 0 : getKey().hashCode()) ^
      (getValue() == null ? 0 : getValue().hashCode());
  }

  /**
   * Compares this pair to another based on the two elements.
   *
   * @param obj the object to compare to, null returns false
   * @return true if the elements of the pair are equal
   */
  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof Map.Entry<?, ?>) {
      final Map.Entry<?, ?> other = (Map.Entry<?, ?>) obj;
      return Objects.equals(getKey(), other.getKey())
        && Objects.equals(getValue(), other.getValue());
    }
    return false;
  }


}
