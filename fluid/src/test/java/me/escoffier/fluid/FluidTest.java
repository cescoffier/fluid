package me.escoffier.fluid;

import io.vertx.core.Vertx;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Outbound;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.framework.Fluid;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.registry.FluidRegistry;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FluidTest {

  @Test
  public void testCreation() {
    Fluid fluid = Fluid.create();
    assertThat(fluid.vertx()).isNotNull();

    AtomicBoolean called = new AtomicBoolean();
    fluid.deploy(f -> called.set(f == fluid));

    assertThat(called.get()).isTrue();
  }

  @Test
  public void testCreationWithVertx() {
    Vertx vertx = Vertx.vertx();
    Fluid fluid = Fluid.create(vertx);

    List<String> list = new ArrayList<>();
    FluidRegistry.register("input", Source.from("a", "b", "c"));
    FluidRegistry.register("output", Sink.<String>forEachPayload(list::add));

    fluid.deploy(f -> {
      Sink<String> output = f.sink("output");
      f.<String>from("input")
        .mapPayload(String::toUpperCase)
        .to(output);
    });

    await().until(() -> list.size() == 3);
    assertThat(list).containsExactly("A", "B", "C");
  }

  @Test
  public void testDeploymentWithClass() {

    List<String> list = new ArrayList<>();
    FluidRegistry.register("input", Source.from("a", "b", "c"));
    FluidRegistry.register("output", Sink.<String>forEachPayload(list::add));

    Fluid fluid = Fluid.create();
    assertThat(fluid.vertx()).isNotNull();

    fluid.deploy(MyMediator1.class);

    await().until(() -> list.size() == 3);
    assertThat(list).containsExactly("A", "B", "C");
  }

  @Test
  public void testDeploymentWithClassUsingPrivate() {

    List<String> list = new ArrayList<>();
    FluidRegistry.register("input", Source.from("a", "b", "c"));
    FluidRegistry.register("output", Sink.<String>forEachPayload(list::add));

    Fluid fluid = Fluid.create();
    assertThat(fluid.vertx()).isNotNull();

    fluid.deploy(MyMediator2.class);

    await().until(() -> list.size() == 3);
    assertThat(list).containsExactly("A", "B", "C");
  }

  @Test
  public void testDeploymentWithClassWithoutTransformationMethod() {

    FluidRegistry.register("input", Source.from("a", "b", "c"));
    FluidRegistry.register("output", Sink.<String>forEach(s -> {
    }));
    Fluid fluid = Fluid.create();
    assertThat(fluid.vertx()).isNotNull();

    try {
      fluid.deploy(MyMediator3.class);
      fail("Invalid configuration not detected");
    } catch (IllegalArgumentException e) {
      // OK
    }
  }

  @Test
  public void testDeploymentWithClassWithUnknownSource() {

    FluidRegistry.register("input", Source.from("a", "b", "c"));
    FluidRegistry.register("output", Sink.<String>forEach(s -> {
    }));

    Fluid fluid = Fluid.create();
    assertThat(fluid.vertx()).isNotNull();

    try {
      fluid.deploy(MyMediator4.class);
      fail("Invalid configuration not detected");
    } catch (IllegalArgumentException e) {
      // OK
    }
  }

  @Test
  public void testDeploymentWithClassWithUnknownSink() {

    FluidRegistry.register("input", Source.from("a", "b", "c"));
    FluidRegistry.register("output", Sink.<String>forEach(s -> {
    }));

    Fluid fluid = Fluid.create();
    assertThat(fluid.vertx()).isNotNull();

    try {
      fluid.deploy(MyMediator5.class);
      fail("Invalid configuration not detected");
    } catch (IllegalArgumentException e) {
      // OK
    }
  }

  @Test
  public void testDeploymentWithClassUsingParentClass() {

    List<String> list = new ArrayList<>();
    FluidRegistry.register("input", Source.from("a", "b", "c"));
    FluidRegistry.register("output", Sink.<String>forEachPayload(list::add));

    Fluid fluid = Fluid.create();
    assertThat(fluid.vertx()).isNotNull();

    fluid.deploy(MyChildMediator.class);

    await().until(() -> list.size() == 3);
    assertThat(list).containsExactly("A", "B", "C");
  }

  @Test
  public void testDeploymentWithClassUsingParameter() {

    List<String> list = new ArrayList<>();
    FluidRegistry.register("input", Source.from("a", "b", "c"));
    FluidRegistry.register("output", Sink.<String>forEachPayload(list::add));

    Fluid fluid = Fluid.create();
    assertThat(fluid.vertx()).isNotNull();

    fluid.deploy(MyMediator6.class);

    await().until(() -> list.size() == 3);
    assertThat(list).containsExactly("A", "B", "C");
  }

  @Test
  public void testDeploymentWithClassUsingParameterWithUnknown() {

    FluidRegistry.register("input", Source.from("a", "b", "c"));
    FluidRegistry.unregisterSink("output");

    Fluid fluid = Fluid.create();
    assertThat(fluid.vertx()).isNotNull();

    try {
      fluid.deploy(MyMediator7.class);
      fail("Invalid configuration not detected");
    } catch (IllegalArgumentException e) {
      // OK
    }

    FluidRegistry.unregisterSource("input");
    FluidRegistry.register("output", Sink.discard());

    try {
      fluid.deploy(MyMediator8.class);
      fail("Invalid configuration not detected");
    } catch (IllegalArgumentException e) {
      // OK
    }
  }

  @Test
  public void testDeploymentWithClassUsingParameterWithBadParameter() {

    FluidRegistry.register("input", Source.from("a", "b", "c"));
    FluidRegistry.register("output", Sink.discard());

    Fluid fluid = Fluid.create();
    assertThat(fluid.vertx()).isNotNull();

    try {
      fluid.deploy(MyMediator9.class);
      fail("Invalid configuration not detected");
    } catch (IllegalArgumentException e) {
      // OK
    }
  }

  public static class MyMediator1 {

    @Inbound("input")
    Source<String> source;

    @Outbound("output")
    Sink<String> sink;

    @Transformation
    void transform() {
      source.mapPayload(String::toUpperCase).to(sink);
    }

  }

  public static class MyMediator2 {

    @Inbound("input")
    private Source<String> source;

    @Outbound("output")
    private Sink<String> sink;

    @Transformation
    private void transform() {
      source.mapPayload(String::toUpperCase).to(sink);
    }

  }

  static class MyMediator3 {

    @Inbound("input")
    Source<String> source;

    @Outbound("output")
    Sink<String> sink;

  }

  static class MyMediator4 {

    @Inbound("missing-input")
    Source<String> source;

    @Outbound("output")
    Sink<String> sink;

    @Transformation
    void transform() {
      source.mapPayload(String::toUpperCase).to(sink);
    }

  }

  static class MyMediator5 {

    @Inbound("input")
    Source<String> source;

    @Outbound("missing-output")
    Sink<String> sink;

    @Transformation
    void transform() {
      source.mapPayload(String::toUpperCase).to(sink);
    }

  }

  public static class MyMediator6 {

    @Transformation
    void transform(@Inbound("input")
                     Source<String> source,
                   @Outbound("output")
                     Sink<String> sink) {
      source.mapPayload(String::toUpperCase).to(sink);
    }

  }

  static class MyMediator7 {

    @Transformation
    void transform(@Inbound("input")
                     Source<String> source,
                   @Outbound("missing-output")
                     Sink<String> sink) {
      source.mapPayload(String::toUpperCase).to(sink);
    }

  }

  static class MyMediator8 {

    @Transformation
    void transform(@Inbound("missing-input")
                     Source<String> source,
                   @Outbound("output")
                     Sink<String> sink) {
      source.mapPayload(String::toUpperCase).to(sink);
    }

  }

  static class MyMediator9 {

    @Inbound("input")
    Source<String> source;

    @Transformation
    void transform(@Outbound("output") Sink<String> sink,
                   String foo) {
      source.mapPayload(String::toUpperCase).mapPayload(s -> foo).to(sink);
    }

  }

}
