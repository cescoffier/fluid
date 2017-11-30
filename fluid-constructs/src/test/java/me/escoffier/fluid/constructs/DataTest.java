package me.escoffier.fluid.constructs;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class DataTest {

  @Test
  public void createWithoutHeaders() {
    Data<String> data = new Data<>("hello");
    assertThat(data.payload()).isEqualTo("hello");
    assertThat(data.headers()).isEmpty();
  }

  @Test
  public void createWithHeaders() {
    Map<String, Object> headers = new HashMap<>();
    headers.put("foo", "bar");
    Data<String> data = new Data<>("hello", headers);
    assertThat(data.payload()).isEqualTo("hello");
    assertThat(data.headers()).containsExactly(entry("foo", "bar"));
    String h = data.get("foo");
    assertThat(h).isEqualTo("bar");
  }

  @Test
  public void testCreationWithWith() {
    Data<String> d1 = new Data<>("hello")
      .with("aaa", 1).with("bbb", "rrrr");
    assertThat(d1.payload()).isEqualTo("hello");
    int x = d1.get("aaa");
    String y = d1.get("bbb");
    assertThat(x).isEqualTo(1);
    assertThat(y).isEqualTo("rrrr");

    Data<Double> d2 = d1.with(25.5);
    assertThat(d2.payload()).isEqualTo(25.5);
    x = d2.get("aaa");
    y = d2.get("bbb");
    assertThat(x).isEqualTo(1);
    assertThat(y).isEqualTo("rrrr");

    d2 = d2.without("bbb");
    x = d2.get("aaa");
    y = d2.get("bbb");
    assertThat(x).isEqualTo(1);
    assertThat(y).isNull();
  }

}
