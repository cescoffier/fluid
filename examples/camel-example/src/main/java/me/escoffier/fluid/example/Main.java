package me.escoffier.fluid.example;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.framework.Fluid;
import me.escoffier.fluid.models.Source;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static me.escoffier.fluid.registry.FluidRegistry.sink;
import static me.escoffier.fluid.registry.FluidRegistry.source;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Main {


  public static void main(String[] args) throws IOException {
    init();

    Fluid fluid = new Fluid();

    // Sensors
    createSensor();
    createSensor();

    fluid.deploy(CamelComponent.class);
    fluid.deploy(ResultComponent.class);


  }


  private static void createSensor() {
    String id = UUID.randomUUID().toString();
    Random random = new Random();

    Source.fromPayloads(Flowable.interval(1000, TimeUnit.MILLISECONDS).subscribeOn(Schedulers.computation()))
      .mapPayload(l -> new JsonObject().put("uuid", id).put("data", random.nextInt(100)))
      .to(sink("sensor"));
  }

  static void init() throws IOException {
    Properties props = new Properties();
    props.setProperty("zookeeper.connection.timeout.ms", "10000");
    File directory = Testing.Files.createTestingDirectory(System.getProperty("java.io.tmpdir"), true);
    new KafkaCluster().withPorts(2181, 9092).addBrokers(1)
      .usingDirectory(directory)
      .deleteDataUponShutdown(true)
      .withKafkaConfiguration(props)
      .deleteDataPriorToStartup(true)
      .startup();
  }
}
