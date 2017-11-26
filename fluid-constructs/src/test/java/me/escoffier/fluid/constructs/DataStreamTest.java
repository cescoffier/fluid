package me.escoffier.fluid.constructs;


import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DataStreamTest {


  @Test
  public void testOf() {
    DataStream<String> stream = DataStream.of(String.class);
    assertThat(stream.isConnectable()).isTrue();
    CacheSink<String> sink = new CacheSink<>();
    stream.connect(Source.from("a", "b", "c"));
    stream.transform(String::toUpperCase).to(sink);
    assertThat(sink.buffer).containsExactly("A", "B", "C");
  }

}
