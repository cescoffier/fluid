package me.escoffier.fluid.models;

import org.junit.Test;

import java.util.AbstractMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the behavior of {@link Pair}.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class PairTest {

  @Test
  public void testSimplePair() {
    Pair<String, Integer> pair = Pair.pair("a", 1);
    assertThat(pair.getKey()).isEqualTo("a");
    assertThat(pair.getValue()).isEqualTo(1);
    assertThat(pair.left()).isEqualTo("a");
    assertThat(pair.right()).isEqualTo(1);
  }

  @Test
  public void testCreationUsingSetters() {
    Pair<String, Integer> pair = Pair.pair("a", 1);
    Pair<String, Integer> another = pair.setLeft("b")
      .setRight(2);
    assertThat(another.left()).isEqualTo("b");
    assertThat(another.right()).isEqualTo(2);

    assertThat(pair).isNotSameAs(another);
  }

  @Test
  public void testEqualsAndHashcode() {
    Pair<String, Integer> pair1 = Pair.pair("a", 1);
    Pair<String, Integer> pair2 = Pair.pair("a", 1);
    Pair<String, Integer> pair3 = Pair.pair("b", 1);

    assertThat(pair1).isEqualTo(pair2).isNotSameAs(pair2);
    assertThat(pair1).isNotEqualTo(pair3).isNotSameAs(pair3);
    assertThat(pair1.hashCode()).isEqualTo(pair2.hashCode()).isNotEqualTo(pair3.hashCode());

    assertThat(pair1).isEqualTo(pair1);

    // Test with a Map.Entry

    Map.Entry<String, Integer> entry = new AbstractMap.SimpleImmutableEntry<>("a", 1);

    assertThat(pair1).isEqualTo(entry);
    entry = new AbstractMap.SimpleImmutableEntry<>("a", 2);
    assertThat(pair1).isNotEqualTo(entry);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testThatSetValueCannotBeCalled() {
    Pair<String, Integer> pair1 = Pair.pair("a", 1);
    pair1.setValue(2);
  }

}
