package me.escoffier.fluid.core.sensor;

import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;
import io.vertx.reactivex.core.AbstractVerticle;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Sensor extends AbstractVerticle {

    private Random random = new Random();

    private String uuid = UUID.randomUUID().toString();

    @Override
    public void start() throws Exception {
        String name = config().getString("name");
        Map config = config().getMap();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonObjectSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonObjectSerializer.class);
        KafkaWriteStream<String, JsonObject> stream = KafkaWriteStream.create(vertx.getDelegate(), config);
        stream.exceptionHandler(Throwable::printStackTrace);
        vertx.setPeriodic(100, l -> {
            JsonObject data = new JsonObject();
            data
                .put("id", uuid)
                .put("name", name)
                .put("value", random.nextInt());
            stream.write(new ProducerRecord("sensor-" + name, data));
        });
    }
}
