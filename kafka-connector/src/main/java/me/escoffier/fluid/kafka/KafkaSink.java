package me.escoffier.fluid.kafka;

import io.reactivex.Completable;
import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.impl.AsyncResultCompletable;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.Message;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.spi.DataExpression;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.LinkedHashMap;
import java.util.Map;

import static me.escoffier.fluid.impl.DataExpressionFactories.requiredEventExpression;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSink<T> implements Sink<T> {

  private final KafkaWriteStream<String, T> stream;
  private final String topic;
  private final Integer partition;
  private final DataExpression key;
  private final String name;
  private final String brokers;
  private Long timestamp;

  public KafkaSink(Vertx vertx, String name, Config config) {
    Map<String, Object> map = toMap(config);
    stream = KafkaWriteStream.create(vertx.getDelegate(), map);
    topic = config.getString("topic", name);
    partition = config.getInt("partition", 0);
    timestamp = config.getLong("timestamp").orElse(null);
    key = requiredEventExpression(config.getString("key", null));
    brokers = map.get("bootstrap.servers").toString();
    this.name = name;
  }

  public String topic() {
    return topic;
  }

  public String brokers() {
    return brokers;
  }

  private static Map<String, Object> toMap(Config config) {
    Map<String, Object> map = new LinkedHashMap<>();
    // Read the global kafka config
    Config kafka = config.root().getConfig("kafka").orElse(Config.empty());
    kafka.names().forEachRemaining(name -> map.put(name, kafka.getString(name).orElse(null)));
    // Override values
    config.names().forEachRemaining(name -> map.put(name, config.getString(name).orElse(null)));
    return map;
  }


  @Override
  public Completable dispatch(Message<T> message) {
    // TODO Override publication configuration using headers
    // TODO Modify the evaluation to support Data<T>
    ProducerRecord<String, T> record
      = new ProducerRecord(topic, partition, timestamp, key.evaluate(message), message.payload());
    return new AsyncResultCompletable(
      handler ->
        stream.write(record, x -> {
          if (x.succeeded()) {
            handler.handle(Future.succeededFuture());
          } else {
            x.cause().printStackTrace();
            handler.handle(Future.failedFuture(x.cause()));
          }
        }));
  }

  @Override
  public String name() {
    return name;
  }
}
