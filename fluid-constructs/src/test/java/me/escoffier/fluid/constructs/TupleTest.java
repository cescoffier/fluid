package me.escoffier.fluid.constructs;

import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Checks the behavior of the {@link Tuple} class
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class TupleTest {

  @Test
  public void testCreation() {
    Tuple tuple = Tuple.tuple("a", "b", "c");
    assertThat(tuple.size()).isEqualTo(3);
    assertThat(tuple.asList()).containsExactly("a", "b", "c");
    assertThat(tuple.<String>nth(0)).isEqualTo("a");
    assertThat(tuple.<String>nth(1)).isEqualTo("b");
    assertThat(tuple.<String>nth(2)).isEqualTo("c");
    assertThat(tuple.contains("b")).isTrue();
    assertThat(tuple.containsAll("a", "c")).isTrue();
    assertThat(tuple.containsAll(Collections.singletonList("c"))).isTrue();
  }

  @Test
  public void testEmpty() {
    Tuple tuple = Tuple.tuple();
    assertThat(tuple.size()).isEqualTo(0);
    assertThat(tuple.asList()).isEmpty();
    assertThat(tuple.contains("b")).isFalse();
    assertThat(tuple.containsAll("a", "c")).isFalse();
    assertThat(tuple.containsAll(Collections.singletonList("c"))).isFalse();
    try {
      tuple.<String>nth(0);
      fail("Index Out Of Bound not detected");
    } catch (IndexOutOfBoundsException e) {
      // OK
    }
  }

  @Test
  public void testCreationWithExplicitNull() {
    Tuple tuple = Tuple.tuple(null);
    assertThat(tuple.size()).isEqualTo(0);
    assertThat(tuple.asList()).isEmpty();
    assertThat(tuple.contains("b")).isFalse();
    assertThat(tuple.containsAll("a", "c")).isFalse();
    assertThat(tuple.containsAll(Collections.singletonList("c"))).isFalse();
    try {
      tuple.<String>nth(0);
      fail("Index Out Of Bound not detected");
    } catch (IndexOutOfBoundsException e) {
      // OK
    }
  }

  @Test
  public void testTupleWithDifferentTypes() {
    Tuple tuple = Tuple.tuple("a", 1, 25.6, "c");
    assertThat(tuple.size()).isEqualTo(4);
    assertThat(tuple.asList()).containsExactly("a", 1, 25.6, "c");
    assertThat(tuple.<String>nth(0)).isEqualTo("a");
    assertThat(tuple.<Integer>nth(1)).isEqualTo(1);
    assertThat(tuple.<Double>nth(2)).isEqualTo(25.6);
    assertThat(tuple.<String>nth(3)).isEqualTo("c");

    assertThat(tuple.contains("b")).isFalse();
    assertThat(tuple.contains(1)).isTrue();

    assertThat(tuple.indexOf("1")).isEqualTo(-1);
    assertThat(tuple.indexOf(1)).isEqualTo(1);
    assertThat(tuple.lastIndexOf(1)).isEqualTo(1);
    assertThat(tuple.lastIndexOf("1")).isEqualTo(-1);
  }

  @Test
  public void testTupleWithDuplicates() {
    Tuple tuple = Tuple.tuple("a", "b", "a", "b", "c", "d", "c");
    assertThat(tuple.indexOf("a")).isEqualTo(0);
    assertThat(tuple.lastIndexOf("a")).isEqualTo(2);

    assertThat(tuple.indexOf("c")).isEqualTo(4);
    assertThat(tuple.lastIndexOf("c")).isEqualTo(6);
  }

  @Test
  public void testHashAndEquals() {
    Tuple t1 = Tuple.tuple("a", "b", 1, 2, 3);
    Tuple t2 = Tuple.tuple("a", "b", 1, 2, 3);
    Tuple t3 = Tuple.tuple("a", 1, 2, 3, "b");

    assertThat((Object) t1).isEqualTo(t2).isNotSameAs(t2).isNotEqualTo(t3).isNotSameAs(t3);
    assertThat(t1.hashCode()).isEqualTo(t2.hashCode()).isNotEqualTo(t3.hashCode());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTupleWithNull() {
    Tuple.tuple("a", "b", null, "c");
  }

}
