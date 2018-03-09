package me.escoffier.fluid.models;

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
    Message<String> message = new Message<>("hello");
    assertThat(message.payload()).isEqualTo("hello");
    assertThat(message.headers()).isEmpty();
  }

  @Test
  public void createWithHeaders() {
    Map<String, Object> headers = new HashMap<>();
    headers.put("foo", "bar");
    Message<String> message = new Message<>("hello", headers);
    assertThat(message.payload()).isEqualTo("hello");
    assertThat(message.headers()).containsExactly(entry("foo", "bar"));
    String h = message.get("foo");
    assertThat(h).isEqualTo("bar");
  }

  @Test
  public void testCreationWithWith() {
    Message<String> d1 = new Message<>("hello")
      .with("aaa", 1).with("bbb", "rrrr");
    assertThat(d1.payload()).isEqualTo("hello");
    int x = d1.get("aaa");
    String y = d1.get("bbb");
    assertThat(x).isEqualTo(1);
    assertThat(y).isEqualTo("rrrr");

    Message<Double> d2 = d1.with(25.5);
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

  @Test
  public void testToString() {
    Message<String> message = new Message<>("hello");
    assertThat(message.toString()).contains("\"payload\":\"hello\"").contains("\"headers\":{}");
    assertThat(message.with("key", "value").toString())
      .contains("\"payload\":\"hello\"")
      .contains("\"headers\":{\"key\":\"value\"}");

    String actual = message.with("key", "value").with("foo", 25).toString();
    assertThat(actual)
      .isEqualTo("{\"payload\":\"hello\", \"headers\":{\"key\":\"value\",\"foo\":\"25\"}}");
  }

}
