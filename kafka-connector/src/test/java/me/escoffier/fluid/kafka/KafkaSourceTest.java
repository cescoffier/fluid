package me.escoffier.fluid.kafka;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.constructs.Sink;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSourceTest {

    private Vertx vertx;

    @ClassRule
    public static GenericContainer kafka =
        new GenericContainer("teivah/kafka:latest")
            .withExposedPorts(2181, 9092)
            .withEnv("ADVERTISED_HOST", "localhost")
            .withEnv("ADVERTISED_PORT", "9092");

    @Before
    public void setup() {
        vertx = Vertx.vertx();
    }

    @After
    public void tearDown() {
        vertx.close();
    }

    @Test
    public void testSource() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();

        KafkaSource<Integer> source = new KafkaSource<>(vertx,
            getKafkaConfig()
                .put("topic", topic)
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("value.deserializer", IntegerDeserializer.class.getName())
        );

        List<Integer> results = new ArrayList<>();
        source
            .transform(i -> i + 1)
            .to(Sink.forEach(results::add));

        AtomicInteger counter = new AtomicInteger();
        usage.produceIntegers(10, null,
            () -> new ProducerRecord<>(topic, counter.getAndIncrement()));

        await().until(() -> results.size() >= 10);
        assertThat(results).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    }


    private JsonObject getKafkaConfig() {
        String randomId = UUID.randomUUID().toString();
        return new JsonObject()
            .put("bootstrap.servers", "localhost:9092")
            .put("enable.auto.commit", false)
            .put("group.id", randomId)
            .put("auto.offset.reset", "earliest")
            .put("key.serializer", StringSerializer.class.getName())
            .put("key.deserializer", StringDeserializer.class.getName());
    }


}