package me.escoffier.fluid.kafka;

import io.reactivex.Completable;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
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
  private Long timestamp;

  public KafkaSink(Vertx vertx, String name, Config config) {
    stream = KafkaWriteStream.create(vertx.getDelegate(), toMap(config));
    topic = config.getString("topic", name);
    partition = config.getInt("partition", 1);
    timestamp = config.getLong("timestamp").orElse(null);
    key = requiredEventExpression(config.getString("key"));
    this.name = name;
  }

  private static Map<String, Object> toMap(Config config) {
    Map<String, Object> map = new LinkedHashMap<>();
    config.names().forEachRemaining(name -> map.put(name, config.getString(name, null)));
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
            handler.handle(Future.failedFuture(x.cause()));
          }
        }));
  }

  @Override
  public String name() {
    return name;
  }
}
