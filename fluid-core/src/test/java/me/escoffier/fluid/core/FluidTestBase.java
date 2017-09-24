package me.escoffier.fluid.core;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
@RunWith(VertxUnitRunner.class)
public class FluidTestBase {

    protected KafkaCluster kafkaCluster;
    protected Vertx vertx;

    protected List<KafkaWriteStream> producers = new ArrayList<>();
    protected List<KafkaConsumer> consumers = new ArrayList<>();

    @Before
    public void setUp(TestContext tc) throws IOException {
        vertx = Vertx.vertx();
        vertx.exceptionHandler(tc.exceptionHandler());

        // Kafka setup for the example
        File dataDir = Testing.Files.createTestingDirectory("cluster");
        dataDir.deleteOnExit();
        kafkaCluster = new KafkaCluster()
            .usingDirectory(dataDir)
            .withPorts(2181, 9092)
            .addBrokers(1)
            .deleteDataPriorToStartup(true)
            .startup();
    }

    @After
    public void tearDown() {
        kafkaCluster.shutdown();

        producers.forEach(KafkaWriteStream::close);
        consumers.forEach(KafkaConsumer::close);

        vertx.close();

        System.clearProperty("fluid-config");
    }
}
