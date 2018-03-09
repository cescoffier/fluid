package me.escoffier.fluid.kafka;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import me.escoffier.fluid.models.Sink;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.reactivex.Completable.complete;
import static me.escoffier.fluid.models.CommonHeaders.key;
import static me.escoffier.fluid.models.CommonHeaders.original;
import static me.escoffier.fluid.kafka.KafkaSourceConfig.kafkaSourceConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
@RunWith(VertxUnitRunner.class)
public class KafkaSourceTest {

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
  public void testSource() throws InterruptedException {
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();
    List<Integer> results = new ArrayList<>();
    KafkaSource<Integer> source = new KafkaSource<>(vertx,
      getKafkaConfig()
        .put("topic", topic)
        .put("value.serializer", IntegerSerializer.class.getName())
        .put("value.deserializer", IntegerDeserializer.class.getName())
    );
    source
      .mapPayload(i -> i + 1)
      .to(Sink.forEachPayload(results::add));

    AtomicInteger counter = new AtomicInteger();
    usage.produceIntegers(10, null,
      () -> new ProducerRecord<>(topic, counter.getAndIncrement()));

    await().atMost(1, TimeUnit.MINUTES).until(() -> results.size() >= 10);
    assertThat(results).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  @Test
  public void testCommonHeaders(TestContext context) throws InterruptedException {
    Async async = context.async();
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();

    KafkaSource<Integer> source = new KafkaSource<>(vertx,
      getKafkaConfig()
        .put("topic", topic)
        .put("value.serializer", IntegerSerializer.class.getName())
        .put("value.deserializer", IntegerDeserializer.class.getName())
    );

    source
      .to(data -> {
        KafkaConsumerRecord record = original(data);
        assertThat(record).isNotNull();
        assertThat(key(data)).isNotNull();
        async.complete();
        return complete();
      });

    usage.produceIntegers(1, null,
      () -> new ProducerRecord<>(topic, "key", 1));
  }

  @Test
  public void testMulticastWithBufferSize() throws InterruptedException {
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();

    KafkaSource<Integer> source = new KafkaSource<>(vertx,
      getKafkaConfig()
        .put("topic", topic)
        .put("value.serializer", IntegerSerializer.class.getName())
        .put("value.deserializer", IntegerDeserializer.class.getName())
        .put("multicast.buffer.size", 20)
    );

    assertThat(source).isNotNull();
    checkMulticast(usage, topic, source);

  }

  private void checkMulticast(KafkaUsage usage, String topic, KafkaSource<Integer> source) {
    List<Integer> resultsA = new ArrayList<>();
    List<Integer> resultsB = new ArrayList<>();
    source
      .mapPayload(i -> i + 1)
      .to(Sink.forEachPayload(resultsB::add));

    source
      .mapPayload(i -> i + 1)
      .to(Sink.forEachPayload(resultsA::add));

    AtomicInteger counter = new AtomicInteger();
    usage.produceIntegers(10, null,
      () -> new ProducerRecord<>(topic, counter.getAndIncrement()));

    await().atMost(1, TimeUnit.MINUTES).until(() -> resultsA.size() >= 10);
    await().atMost(1, TimeUnit.MINUTES).until(() -> resultsB.size() >= 10);
    assertThat(resultsA).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    assertThat(resultsB).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  @Test
  public void testMulticastWithTime() throws InterruptedException {
    KafkaUsage usage = new KafkaUsage();
    String topic = UUID.randomUUID().toString();

    KafkaSource<Integer> source = new KafkaSource<>(vertx,
      getKafkaConfig()
        .put("topic", topic)
        .put("value.serializer", IntegerSerializer.class.getName())
        .put("value.deserializer", IntegerDeserializer.class.getName())
        .put("multicast.buffer.period.ms", 2000)
    );
    assertThat(source).isNotNull();
    checkMulticast(usage, topic, source);

  }

  private JsonObject getKafkaConfig() {
    String randomId = UUID.randomUUID().toString();
    return kafkaSourceConfig("name").
      bootstrapServers("localhost:9092").
      enableAutoCommit(false).
      groupId(randomId).
      autoOffsetReset("earliest").
      keyDeserializer(StringDeserializer.class).
      valueDeserializer(StringDeserializer.class).
      build();
  }
}
