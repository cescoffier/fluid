package me.escoffier.fluid.core;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
@RunWith(VertxUnitRunner.class)
public class SimpleTest extends FluidTestBase {


    @Test
    public void test(TestContext tc) {
        System.setProperty("fluid-config", "config/simple.yaml");
        Async async = tc.async();

        vertx.rxDeployVerticle(SimpleProcessor.class.getName())
            .subscribe();

        producers.add(producer("input"));


        KafkaConsumer<String, JsonObject> consumer = consumer();
        consumers.add(consumer);

        kafkaCluster.useTo().consumeStrings("output", 4, 60, TimeUnit.SECONDS, async::complete,
            (x, value) -> {
                JsonObject data = new JsonObject(value);
                assertThat(data.getLong("timestamp")).isNotNegative().isNotZero().isNotNull();
                assertThat(data.getString("const")).isEqualToIgnoringCase("a constant value");
                assertThat(data.getDouble("val")).isNotNegative().isNotZero().isNotNull();
                return true;
            });

    }

    private KafkaWriteStream<String, JsonObject> producer(String topic) {
        Properties config = kafkaCluster.useTo().getProducerProperties("testProduce_producer");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonObjectSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonObjectSerializer.class);

        KafkaWriteStream<String, JsonObject> stream = KafkaWriteStream.create(vertx.getDelegate(), config);
        stream.exceptionHandler(Throwable::printStackTrace);
        vertx.setPeriodic(1000, l -> {
            stream.write(new ProducerRecord<>(topic,
                new JsonObject().put("const", "a constant value")
                    .put("val", Math.random() * 100))
            );
        });
        return stream;
    }

    private KafkaConsumer<String, JsonObject> consumer() {
        System.setProperty("fluid-config", "config/sensor.yaml");

        Properties properties = kafkaCluster.useTo()
            .getConsumerProperties("the_group", "the_client", OffsetResetStrategy.LATEST);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return KafkaConsumer.create(vertx, (Map) properties);
    }
}
