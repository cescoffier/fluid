package me.escoffier.fluid.kafka;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSource<T> extends DataStreamImpl<Void, T> implements Source<T> {
  private final String name;

  public KafkaSource(Vertx vertx, JsonObject json) {
    super(null, KafkaConsumer.<String, T>create(vertx, toMap(json))
      .subscribe(json.getString("topic", json.getString("name")))
      .toFlowable()
      .map(KafkaSource::createDataFromRecord)
    );

    name = json.getString("name");
  }

  private static <T> Data<T> createDataFromRecord(KafkaConsumerRecord<String, T> record) {
    // TODO need another API to avoid creating so many objects.
    return new Data<>(record.value())
      .with("timestamp", record.timestamp())
      .with("timestamp-type", record.timestampType())
      .with("record", record)
      .with("partition", record.partition())
      .with("checksum", record.checksum())
      .with("key", record.key())
      .with("topic", record.topic());
  }

  private static Map<String, String> toMap(JsonObject json) {
    Map<String, String> map = new LinkedHashMap<>();
    json.forEach(entry -> map.put(entry.getKey(), entry.getValue().toString()));
    return map;
  }

  @Override
  public String name() {
    return name;
  }
}
