package me.escoffier.fluid.kafka;

import me.escoffier.fluid.framework.Fluid;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.registry.FluidRegistry;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the configuration of the sources and sinks from the configuration file
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaConfigurationTest {

  private Fluid fluid;

  @After
  public void tearDown() {
    System.clearProperty("fluid-config");
    if (fluid != null) {
      fluid.close();
    }
    FluidRegistry.reset();
  }

  @Test
  public void testSourceConfigUsingExplodedConf() {
    System.setProperty("fluid-config", "src/test/resources/config/source-and-sink.yaml");

    fluid = Fluid.create();
    Source<Object> sensor = fluid.from("sensor");
    Source<Object> average = fluid.from("average");

    assertThat(sensor).isInstanceOf(KafkaSource.class);
    assertThat(average).isInstanceOf(KafkaSource.class);

    assertThat(sensor.attr("kafka-topic")).get().isEqualTo("sensor");
    assertThat(sensor.attr("kafka-broker")).get().isEqualTo("localhost:9092");
    assertThat(sensor.name()).isEqualTo("sensor");

    assertThat(average.attr("kafka-topic")).get().isEqualTo("average");
    assertThat(average.attr("kafka-broker")).get().isEqualTo("localhost:9092");
    assertThat(average.name()).isEqualTo("average");
  }

  @Test
  public void testSinkConfigUsingExplodedConf() {
    System.setProperty("fluid-config", "src/test/resources/config/source-and-sink.yaml");

    fluid = Fluid.create();
    Sink<Object> sensor = fluid.sink("sensor");
    Sink<Object> average = fluid.sink("average");

    assertThat(sensor).isInstanceOf(KafkaSink.class);
    assertThat(average).isInstanceOf(KafkaSink.class);

    assertThat(((KafkaSink) sensor).topic()).isEqualTo("sensor");
    assertThat(((KafkaSink) sensor).brokers()).isEqualTo("localhost:9092");
    assertThat(sensor.name()).isEqualTo("sensor");

    assertThat(((KafkaSink) average).topic()).isEqualTo("average");
    assertThat(((KafkaSink) average).brokers()).isEqualTo("localhost:9092");
    assertThat(average.name()).isEqualTo("average");
  }

  @Test
  public void testSourceConfigUsingGlobalConf() {
    System.setProperty("fluid-config", "src/test/resources/config/use-global.yaml");

    fluid = Fluid.create();
    Source<Object> sensor = fluid.from("sensor");
    Source<Object> average = fluid.from("average");

    assertThat(sensor).isInstanceOf(KafkaSource.class);
    assertThat(average).isInstanceOf(KafkaSource.class);

    assertThat(sensor.attr("kafka-topic")).get().isEqualTo("sensor");
    assertThat(sensor.attr("kafka-broker")).get().isEqualTo("localhost:9092");
    assertThat(sensor.name()).isEqualTo("sensor");

    assertThat(average.attr("kafka-topic")).get().isEqualTo("average");
    assertThat(average.attr("kafka-broker")).get().isEqualTo("localhost:9092");
    assertThat(average.name()).isEqualTo("average");
  }

  @Test
  public void testSinkConfigUsingGlobalConf() {
    System.setProperty("fluid-config", "src/test/resources/config/use-global.yaml");

    fluid = Fluid.create();
    Sink<Object> sensor = fluid.sink("sensor");
    Sink<Object> average = fluid.sink("average");

    assertThat(sensor).isInstanceOf(KafkaSink.class);
    assertThat(average).isInstanceOf(KafkaSink.class);

    assertThat(((KafkaSink) sensor).topic()).isEqualTo("sensor");
    assertThat(((KafkaSink) sensor).brokers()).isEqualTo("localhost:9092");
    assertThat(sensor.name()).isEqualTo("sensor");

    assertThat(((KafkaSink) average).topic()).isEqualTo("average");
    assertThat(((KafkaSink) average).brokers()).isEqualTo("localhost:9092");
    assertThat(average.name()).isEqualTo("average");
  }


}
