package me.escoffier.fluid.kafka;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.constructs.Source;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSinkTest {

    private Vertx vertx;

    private static KafkaCluster kafka;

    @BeforeClass
    public static void beforeClass() throws IOException {
        Properties props = new Properties();
        props.setProperty("zookeeper.connection.timeout.ms", "10000");
        File directory = Testing.Files.createTestingDirectory(System.getProperty("java.io.tmpdir"), true);
        kafka = new KafkaCluster().withPorts(2182, 9092).addBrokers(1)
            .usingDirectory(directory)
            .deleteDataUponShutdown(true)
            .withKafkaConfiguration(props)
            .deleteDataPriorToStartup(true)
            .startup();
    }

    @AfterClass
    public static void afterClass() {
        kafka.shutdown();
    }

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
            .transformItem(i -> i + 1)
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
        Source.fromItems(stream)
            .onItem(values::add)
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
