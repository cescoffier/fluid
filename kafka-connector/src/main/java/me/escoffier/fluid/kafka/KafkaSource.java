package me.escoffier.fluid.kafka;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import me.escoffier.fluid.models.AbstractSource;
import me.escoffier.fluid.models.Message;
import me.escoffier.fluid.models.Source;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static me.escoffier.fluid.models.CommonHeaders.ADDRESS;
import static me.escoffier.fluid.models.CommonHeaders.KEY;
import static me.escoffier.fluid.models.CommonHeaders.ORIGINAL;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSource<T> extends AbstractSource<T> implements Source<T> {

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
      }
    ), json.getString("name"), null);
  }

  private static <T> Message<T> createDataFromRecord(KafkaConsumerRecord<String, T> record) {
    Map<String, Object> headers = new HashMap<>();
    headers.put("timestamp", record.timestamp());
    headers.put("timestamp-type", record.timestampType());
    headers.put(ORIGINAL, record);
    headers.put("partition", record.partition());
    headers.put("checksum", record.checksum());
    headers.put(KEY, record.key());
    headers.put(ADDRESS, record.topic());
    return new Message<>(record.value(), headers);
  }

  private static Map<String, String> toMap(JsonObject json) {
    Map<String, String> map = new LinkedHashMap<>();
    json.forEach(entry -> map.put(entry.getKey(), entry.getValue().toString()));
    return map;
  }

}
