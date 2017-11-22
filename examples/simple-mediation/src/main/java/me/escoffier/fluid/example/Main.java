package me.escoffier.fluid.example;

import hu.akarnokd.rxjava2.math.MathFlowable;
import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Sinks;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.constructs.Sources;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Main {


    private static KafkaCluster kafkaCluster;

    public static void main(String[] args) throws IOException {
        init();

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(WebVerticle.class.getName());

        Sources.load(vertx);
        Sinks.load(vertx);

        // Sensors
        createSensor();
        createSensor();

        // Mediation
        Sources.get("sensor", JsonObject.class)
            .transform(json -> json.getDouble("data"))
            .transformFlow(flow ->
                flow.window(5)
                    .flatMap(MathFlowable::averageDouble))
            .broadcastTo(Sink.forEach(System.out::println), Sinks.get("eb-average"));

//         //Output
//        printAndSend(vertx);

    }
    

    private static void createSensor() {
        String id = UUID.randomUUID().toString();
        Random random = new Random();

        Source.from(Flowable.interval(1000, TimeUnit.MILLISECONDS).subscribeOn(Schedulers.computation()))
            .transform(l -> new JsonObject().put("uuid", id).put("data", random.nextInt(100)))
            .to(Sinks.get("sensor"));
    }

    public static void init() throws IOException {
        Properties props = new Properties();
        props.setProperty("zookeeper.connection.timeout.ms", "10000");
        File directory = Testing.Files.createTestingDirectory(System.getProperty("java.io.tmpdir"), true);
        kafkaCluster = new KafkaCluster().withPorts(2181, 9092).addBrokers(1)
            .usingDirectory(directory)
            .deleteDataUponShutdown(true)
            .withKafkaConfiguration(props)
            .deleteDataPriorToStartup(true)
            .startup();
    }
}
