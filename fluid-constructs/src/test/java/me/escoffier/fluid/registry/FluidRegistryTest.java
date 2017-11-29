package me.escoffier.fluid.registry;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.constructs.impl.SourceImpl;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FluidRegistryTest {

  @After
  public void tearDown() {
    FluidRegistry.reset();
  }

  @Test
  public void testRegistrationOfSink() {
    Sink<String> discard = Sink.discard();
    FluidRegistry.register("some-name", discard);
    assertThat(FluidRegistry.sink("some-name")).isSameAs(discard);
    assertThat(FluidRegistry.sink("unknown")).isNull();
  }

  @Test(expected = NullPointerException.class)
  public void testRegistrationOfSinkWithNullName() {
    Sink<String> discard = Sink.discard();
    FluidRegistry.register(null, discard);
  }

  @Test
  public void testRegistrationOfSource() {
    Source<String> source = Source.empty();
    FluidRegistry.register("some-name", source);
    assertThat(FluidRegistry.source("some-name")).isSameAs(source);
    assertThat(FluidRegistry.source("unknown")).isNull();
  }

  @Test(expected = NullPointerException.class)
  public void testRegistrationOfSourceWithNullName() {
    Source<String> source = Source.empty();
    FluidRegistry.register(null, source);
  }

  @Test
  public void testRegistrationOfNamedSource() {
    Source<String> source = new SourceImpl<String>(Flowable.empty()) {
      @Override
      public String name() {
        return "my-source";
      }
    };

    FluidRegistry.register(source);
    assertThat(FluidRegistry.source("my-source")).isSameAs(source);
    assertThat(FluidRegistry.source("my-source", String.class)).isSameAs(source);
  }

  @Test
  public void testRegistrationOfNamedSink() {
    Sink<String> source = new Sink<String>() {

      @Override
      public Completable dispatch(Data<String> data) {
        return Completable.complete();
      }

      @Override
      public String name() {
        return "my-sink";
      }
    };

    FluidRegistry.register(source);
    assertThat(FluidRegistry.sink("my-sink")).isSameAs(source);
  }

  @Test
  public void testUnRegistrationByObject() {
    Source<String> source = Source.empty();
    Sink<String> discard = Sink.discard();

    FluidRegistry.register("foo", source);
    FluidRegistry.register("foo", discard);

    assertThat(FluidRegistry.source("foo")).isEqualTo(source);
    assertThat(FluidRegistry.sink("foo")).isEqualTo(discard);

    FluidRegistry.unregisterSource("foo");
    FluidRegistry.unregisterSink("foo");

    assertThat(FluidRegistry.source("foo")).isNull();
    assertThat(FluidRegistry.sink("foo")).isNull();

  }

  @Test
  public void testInitialize() {
    Vertx vertx = Vertx.vertx();
    FluidRegistry.initialize(vertx);
    vertx.close();
  }

}
