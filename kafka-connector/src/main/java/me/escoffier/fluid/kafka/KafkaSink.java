package me.escoffier.fluid.kafka;

import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.impl.AsyncResultCompletable;
import me.escoffier.fluid.constructs.Sink;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSink<T> implements Sink<T> {

    private final KafkaWriteStream<String, T> stream;
    private final String topic;
    private final Integer partition;
    private final String key;
    private final String name;
    private Long timestamp;

    public KafkaSink(Vertx vertx, JsonObject json) {
        stream = KafkaWriteStream.create(vertx.getDelegate(), toMap(json));
        topic = json.getString("topic");
        partition = json.getInteger("partition");
        timestamp = json.getLong("timestamp");
        key = json.getString("key");
        name = json.getString("name");
    }

    private static Map<String, Object> toMap(JsonObject json) {
        Map<String, Object> map = new LinkedHashMap<>();
        json.forEach(entry -> map.put(entry.getKey(), entry.getValue().toString()));
        return map;
    }


    @Override
    public Completable dispatch(T data) {
        ProducerRecord<String, T> record
            = new ProducerRecord(topic, partition, timestamp, key, data);
        return new AsyncResultCompletable(
            handler -> stream.write(record, x -> handler.handle(x.mapEmpty())));
    }

    @Override
    public String name() {
        return name;
    }
}
