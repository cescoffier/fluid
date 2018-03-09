package me.escoffier.fluid.kafka;

import io.reactivex.Completable;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.impl.AsyncResultCompletable;
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

  public KafkaSink(Vertx vertx, JsonObject json) {
    stream = KafkaWriteStream.create(vertx.getDelegate(), toMap(json));
    topic = json.getString("topic");
    partition = json.getInteger("partition");
    timestamp = json.getLong("timestamp");
    key = requiredEventExpression(json.getString("key"));
    name = json.getString("name");
  }

  private static Map<String, Object> toMap(JsonObject json) {
    Map<String, Object> map = new LinkedHashMap<>();
    json.forEach(entry -> map.put(entry.getKey(), entry.getValue().toString()));
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
