package me.escoffier.fluid.kafka;

import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.DefaultSource;
import me.escoffier.fluid.models.Message;
import me.escoffier.fluid.models.Source;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static me.escoffier.fluid.models.CommonHeaders.*;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSource<T> extends DefaultSource<T> implements Source<T> {

  KafkaSource(Vertx vertx, String name, Config config) {
    super(KafkaConsumer.<String, T>create(vertx, toMap(config))
      .subscribe(config.getString("topic", name))
      .toFlowable()
      .map(KafkaSource::createDataFromRecord)
      .compose(upstream -> {
        int size = config.getInt("multicast.buffer.size", 0);
        if (size > 0) {
          return upstream.replay(size).autoConnect();
        }

        Integer seconds = config.getInt("multicast.buffer.period.ms", -1);
        if (seconds != -1) {
          return upstream.replay(seconds, TimeUnit.MILLISECONDS).autoConnect();
        }

        return upstream;
      }
    ),name, getAttributes(config));
  }

  private static Map<String, Object> getAttributes(Config config) {
    Map<String ,Object> map = new HashMap<>();
    Map<String, String> c = toMap(config);
    map.put("kafka-broker", c.get("bootstrap.servers"));
    map.put("kafka-topic", c.get("topic"));
    return map;
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

  private static Map<String, String> toMap(Config config) {
    Map<String, String> map = new LinkedHashMap<>();

    // Read the global kafka config
    Config kafka = config.root().getConfig("kafka").orElse(Config.empty());
    kafka.names().forEachRemaining(name -> map.put(name, kafka.getString(name).orElse(null)));
    // Override values
    config.names().forEachRemaining(name -> map.put(name, config.getString(name).orElse(null)));
    return map;
  }

}
