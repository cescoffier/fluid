package me.escoffier.fluid.kafka;

import org.junit.Test;

import java.util.Map;

import static me.escoffier.fluid.kafka.KafkaSourceConfig.kafkaSourceConfig;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaSourceConfigTest {

  @Test
  public void shouldHaveName() {
    Map<String, Object> config = kafkaSourceConfig("name").build().getMap();
      assertThat(config).containsEntry("name", "name");
  }

  @Test
  public void shouldHaveBootstrapServers() {
    Map<String, Object> config = kafkaSourceConfig("name").bootstrapServers("localhost:9092").build().getMap();
    assertThat(config).containsEntry("bootstrap.servers", "localhost:9092");
  }

}
