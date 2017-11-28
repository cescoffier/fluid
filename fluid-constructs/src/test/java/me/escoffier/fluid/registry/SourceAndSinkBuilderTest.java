package me.escoffier.fluid.registry;

import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.constructs.ListSink;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that sources and sinks can be created from the configuration.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SourceAndSinkBuilderTest {


  private Vertx vertx;

  @Before
  public void setup() {
    System.setProperty("fluid-config", "src/test/resources/config/fake.yml");
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() {
    System.clearProperty("fluid-config");
    vertx.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void loadSourceTest() {
    Map<String, Source> sources = SourceAndSinkBuilder.createSourcesFromConfiguration(vertx);
    assertThat(sources).hasSize(2);

    Source<String> source1 = sources.get("source1");
    assertThat(source1).isNotNull();
    Source<Integer> source2 = sources.get("source2");
    assertThat(source2).isNotNull();

    ListSink<String> list = new ListSink<>();
    source1.to(list);
    assertThat(list.values()).containsExactly("a", "b", "c");
    assertThat(source1.name()).isEqualTo("source1");

    ListSink<Integer> list2 = new ListSink<>();
    source2.to(list2);
    assertThat(list2.values()).containsExactly(0, 1, 2, 3);
    assertThat(source2.name()).isEqualTo("source2");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void loadSinkTest() {
    Map<String, Sink> sinks = SourceAndSinkBuilder.createSinksFromConfiguration(vertx);
    assertThat(sinks).hasSize(2);

    Sink<String> sink1 = sinks.get("sink1");
    assertThat(sink1).isNotNull().isInstanceOf(ListSink.class);
    Sink<Integer> sink2 = sinks.get("sink2");
    assertThat(sink2).isNotNull().isInstanceOf(ListSink.class);
    ;

    Source.from("1", "2", "3").to(sink1);
    Source.from(4, 5).to(sink2);

    assertThat(((ListSink) sink1).values()).containsExactly("1", "2", "3");
    assertThat(((ListSink) sink2).values()).containsExactly(4, 5);
  }

  @Test
  public void testInitializationFromRegistry() {
    FluidRegistry.initialize(vertx);
    assertThat(FluidRegistry.sink("sink1")).isNotNull();
    assertThat(FluidRegistry.sink("sink2")).isNotNull();
    assertThat(FluidRegistry.sink("not a sink")).isNull();

    assertThat(FluidRegistry.source("source1")).isNotNull();
    assertThat(FluidRegistry.source("source2")).isNotNull();
    assertThat(FluidRegistry.source("not a source")).isNull();
  }

}
