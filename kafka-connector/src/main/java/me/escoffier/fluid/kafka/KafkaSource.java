package me.escoffier.fluid.kafka;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.constructs.impl.SourceImpl;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static me.escoffier.fluid.constructs.CommonHeaders.ADDRESS;
import static me.escoffier.fluid.constructs.CommonHeaders.KEY;
import static me.escoffier.fluid.constructs.CommonHeaders.ORIGINAL;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSource<T> extends SourceImpl<T> implements Source<T> {
  private final String name;

  KafkaSource(Vertx vertx, JsonObject json) {
    super(KafkaConsumer.<String, T>create(vertx, toMap(json))
      .subscribe(json.getString("topic", json.getString("name")))
      .toFlowable()
      .map(KafkaSource::createDataFromRecord)
      .compose(upstream -> {
        int size = json.getInteger("multicast.buffer.size", 0);
        if (size > 0) {
          return upstream.replay(size).autoConnect();
        }

        Integer seconds = json.getInteger("multicast.buffer.period.ms", -1);
        if (seconds != -1) {
          return upstream.replay(seconds, TimeUnit.MILLISECONDS).autoConnect();
        }

        return upstream;
      })
    );

    name = json.getString("name");
  }

  private static <T> Data<T> createDataFromRecord(KafkaConsumerRecord<String, T> record) {
    Map<String, Object> headers = new HashMap<>();
    headers.put("timestamp", record.timestamp());
    headers.put("timestamp-type", record.timestampType());
    headers.put(ORIGINAL, record);
    headers.put("partition", record.partition());
    headers.put("checksum", record.checksum());
    headers.put(KEY, record.key());
    headers.put(ADDRESS, record.topic());
    return new Data<>(record.value(), headers);
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
