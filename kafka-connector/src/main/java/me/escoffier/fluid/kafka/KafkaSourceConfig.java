package me.escoffier.fluid.kafka;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaSourceConfig {

  private final JsonObject config;

  public KafkaSourceConfig(JsonObject config) {
    this.config = config;
  }

  public KafkaSourceConfig(String name) {
    this(new JsonObject().put("name", name));
  }

  public static KafkaSourceConfig kafkaSourceConfig(JsonObject config) {
    return new KafkaSourceConfig(config);
  }

  public static KafkaSourceConfig kafkaSourceConfig(String name) {
    return new KafkaSourceConfig(name);
  }

  public JsonObject build() {
    return config;
  }

  public KafkaSourceConfig bootstrapServers(String bootstrapServers) {
    config.put("bootstrap.servers", bootstrapServers);
    return this;
  }

  public KafkaSourceConfig groupId(String groupId) {
    config.put("group.id", groupId);
    return this;
  }

  public KafkaSourceConfig keyDeserializer(Class<? extends Deserializer> keyDeserializer) {
    config.put("key.deserializer", keyDeserializer.getName());
    return this;
  }

  public KafkaSourceConfig valueDeserializer(Class<? extends Deserializer> valueDeserializer) {
    config.put("value.deserializer", valueDeserializer.getName());
    return this;
  }

  public KafkaSourceConfig topic(String topic) {
    config.put("topic", topic);
    return this;
  }

  public KafkaSourceConfig enableAutoCommit(boolean enableAutoCommit) {
    config.put("enable.auto.commit", enableAutoCommit);
    return this;
  }

  public KafkaSourceConfig autoOffsetReset(String autoOffsetReset) {
    config.put("auto.offset.reset", autoOffsetReset);
    return this;
  }

  public KafkaSourceConfig multicastBufferSize(int multicastBufferSize) {
    config.put("multicast.buffer.size", multicastBufferSize);
    return this;
  }

  public KafkaSourceConfig multicastBufferPeriodMs(int multicastBufferPeriodMs) {
    config.put("multicast.buffer.period.ms", multicastBufferPeriodMs);
    return this;
  }

}
