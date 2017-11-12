package me.escoffier.fluid.kafka;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.constructs.Source;
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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSinkTest {

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
    public void testSinkWithInteger() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger expected = new AtomicInteger(2);
        usage.consumeIntegers(topic, 10, 10, TimeUnit.SECONDS,
            latch::countDown,
            (k, v) -> v == expected.getAndIncrement());


        KafkaSink<Integer> sink = new KafkaSink<>(vertx,
            getKafkaConfig()
                .put("topic", topic)
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("value.deserializer", IntegerDeserializer.class.getName())
        );


        Source.from(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .transform(i -> i + 1)
            .to(sink);


        latch.await();
    }

    @Test
    public void testSinkWithString() throws InterruptedException {
        KafkaUsage usage = new KafkaUsage();
        String topic = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        List<String> values = new ArrayList<>();
        usage.consumeStrings(topic, 10, 10, TimeUnit.SECONDS,
            latch::countDown,
            (k, v) -> values.contains(v));

        KafkaSink<String> sink = new KafkaSink<>(vertx,
            getKafkaConfig()
                .put("topic", topic)
                .put("value.serializer", StringSerializer.class.getName())
                .put("value.deserializer", StringDeserializer.class.getName())
        );


        Stream<String> stream = new Random().longs(10).mapToObj(Long::toString);
        Source.from(stream)
            .onData(values::add)
            .to(sink);


        latch.await();
    }


    private JsonObject getKafkaConfig() {
        return new JsonObject()
            .put("bootstrap.servers", "localhost:9092")
            .put("acks", 1)
            .put("key.serializer", StringSerializer.class.getName())
            .put("key.deserializer", StringDeserializer.class.getName());
    }


}