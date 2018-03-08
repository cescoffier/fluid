package me.escoffier.fluid.inject;

import io.reactivex.Flowable;
import me.escoffier.fluid.framework.Fluid;
import me.escoffier.fluid.impl.ListSink;
import me.escoffier.fluid.models.AbstractSource;
import me.escoffier.fluid.models.Data;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.registry.FluidRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class InjectionTest {

  private static final Source<String> MY_SOURCE = new MySource();
  private ListSink<String> my_sink = Sink.list();
  private Fluid fluid;

  @Before
  public void setUp() {
    my_sink = Sink.list();
    fluid = new Fluid();
    FluidRegistry.register("my-source", MY_SOURCE);
    FluidRegistry.register("my-sink", my_sink);
  }

  @After
  public void tearDown() {
    FluidRegistry.unregisterSource("my-source");
    FluidRegistry.unregisterSink("my-sink");
  }

  @Test
  public void testFieldSourceInjection() {
    fluid.deploy(MediatorRequiringSource.class);
    await().until(() -> MediatorRequiringSource.SPY.size() == 5);
    assertThat(MediatorRequiringSource.SPY).containsExactly("A", "B", "C", "D", "E");
  }

  @Test
  public void testFieldPublisherInjection() {
    fluid.deploy(MediatorRequiringPublisher.class);

    await().until(() -> MediatorRequiringPublisher.SPY.size() == 5);
    assertThat(MediatorRequiringPublisher.SPY).containsExactly("A", "B", "C", "D", "E");
  }

  @Test
  public void testFieldFlowableInjection() {
    fluid.deploy(MediatorRequiringFlowable.class);

    await().until(() -> MediatorRequiringFlowable.SPY.size() == 5);
    assertThat(MediatorRequiringFlowable.SPY).containsExactly("A", "B", "C", "D", "E");
  }

  @Test
  public void testFieldUnwrappedPublisherInjection() {
    fluid.deploy(MediatorRequiringUnwrappedPublisher.class);

    await().until(() -> MediatorRequiringUnwrappedPublisher.SPY.size() == 5);
    assertThat(MediatorRequiringUnwrappedPublisher.SPY).containsExactly("A", "B", "C", "D", "E");
  }

  @Test
  public void testFieldUnwrappedFlowableInjection() {
    fluid.deploy(MediatorRequiringUnwrappedFlowable.class);

    await().until(() -> MediatorRequiringUnwrappedFlowable.SPY.size() == 5);
    assertThat(MediatorRequiringUnwrappedFlowable.SPY).containsExactly("A", "B", "C", "D", "E");
  }

  @Test
  public void testFieldSinkInjection() {
    fluid.deploy(MediatorRequiringSink.class);

    await().until(() -> my_sink.values().size() == 5);
    assertThat( my_sink.values()).containsExactly("A", "B", "C", "D", "E");
  }

  @Test
  public void testMethodParameterInjection() {
    fluid.deploy(MediatorWithParameterInjection.class);

    await().until(() -> my_sink.values().size() == 25);
    assertThat(my_sink.values())
      .contains("S1-A", "S1-B", "S1-C", "S1-D", "S1-E")
      .contains("S2-A", "S2-B", "S2-C", "S2-D", "S2-E")
      .contains("S3-A", "S3-B", "S3-C", "S3-D", "S3-E")
      .contains("S4-A", "S4-B", "S4-C", "S4-D", "S4-E")
      .contains("S5-A", "S5-B", "S5-C", "S5-D", "S5-E");
  }

  @Test
  public void testMethodReturningFlowable() {
    fluid.deploy(MediatorProvidingFlowable.class);

    await().until(() -> my_sink.values().size() == 10);
    assertThat( my_sink.values()).containsExactly("A", "A", "B", "B", "C", "C","D", "D", "E", "E");
  }

  @Test
  public void testMethodReturningFlowableOfData() {
    fluid.deploy(MediatorProvidingFlowableData.class);

    await().until(() -> my_sink.values().size() == 10);
    assertThat( my_sink.values()).containsExactly("A", "A", "B", "B", "C", "C","D", "D", "E", "E");
  }

  @Test
  public void testMethodReturningPublisher() {
    fluid.deploy(MediatorProvidingPublisher.class);

    await().until(() -> my_sink.values().size() == 10);
    assertThat( my_sink.values()).containsExactly("A", "A", "B", "B", "C", "C","D", "D", "E", "E");
  }

  @Test
  public void testMethodReturningPublisherOfData() {
    fluid.deploy(MediatorProvidingPublisherData.class);

    await().until(() -> my_sink.values().size() == 10);
    assertThat( my_sink.values()).containsExactly("A", "A", "B", "B", "C", "C","D", "D", "E", "E");
  }

  private static class MySource extends AbstractSource<String> {

    MySource() {
      super(
        Flowable.fromArray(new Data<>("a"), new Data<>("b"), new Data<>("c"), new Data<>("d"), new Data<>("e")),
        "my-source",
        null
      );
    }
  }

}
