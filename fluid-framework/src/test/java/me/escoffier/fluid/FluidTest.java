package me.escoffier.fluid;

import io.vertx.core.Vertx;
import me.escoffier.fluid.annotations.Port;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Sinks;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.constructs.Sources;
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
        Fluid fluid = new Fluid();
        assertThat(fluid.vertx()).isNotNull();

        AtomicBoolean called = new AtomicBoolean();
        fluid.deploy(f -> called.set(f == fluid));

        assertThat(called.get()).isTrue();
    }

    @Test
    public void testCreationWithVertx() {
        Vertx vertx = Vertx.vertx();
        Fluid fluid = new Fluid(vertx);

        List<String> list = new ArrayList<>();
        Sources.register("input", Source.from("a", "b", "c"));
        Sinks.register("output", Sink.<String>forEach(list::add));

        fluid.deploy(f -> {
            Sink<String> output = f.sink("output");
            f.<String>from("input")
                .transform(String::toUpperCase)
                .to(output);
        });

        await().until(() -> list.size() == 3);
        assertThat(list).containsExactly("A", "B", "C");
    }

    @Test
    public void testDeploymentWithClass() {

        List<String> list = new ArrayList<>();
        Sources.register("input", Source.from("a", "b", "c"));
        Sinks.register("output", Sink.<String>forEach(list::add));

        Fluid fluid = new Fluid();
        assertThat(fluid.vertx()).isNotNull();

        fluid.deploy(MyMediator1.class);

        await().until(() -> list.size() == 3);
        assertThat(list).containsExactly("A", "B", "C");
    }

    @Test
    public void testDeploymentWithClassUsingPrivate() {

        List<String> list = new ArrayList<>();
        Sources.register("input", Source.from("a", "b", "c"));
        Sinks.register("output", Sink.<String>forEach(list::add));

        Fluid fluid = new Fluid();
        assertThat(fluid.vertx()).isNotNull();

        fluid.deploy(MyMediator2.class);

        await().until(() -> list.size() == 3);
        assertThat(list).containsExactly("A", "B", "C");
    }

    @Test
    public void testDeploymentWithClassWithoutTransformationMethod() {

        Sources.register("input", Source.from("a", "b", "c"));
        Sinks.register("output", Sink.<String>forEach(s -> {
        }));
        Fluid fluid = new Fluid();
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

        Sources.register("input", Source.from("a", "b", "c"));
        Sinks.register("output", Sink.<String>forEach(s -> {
        }));

        Fluid fluid = new Fluid();
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

        Sources.register("input", Source.from("a", "b", "c"));
        Sinks.register("output", Sink.<String>forEach(s -> {
        }));

        Fluid fluid = new Fluid();
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
        Sources.register("input", Source.from("a", "b", "c"));
        Sinks.register("output", Sink.<String>forEach(list::add));

        Fluid fluid = new Fluid();
        assertThat(fluid.vertx()).isNotNull();

        fluid.deploy(MyChildMediator.class);

        await().until(() -> list.size() == 3);
        assertThat(list).containsExactly("A", "B", "C");
    }

    @Test
    public void testDeploymentWithClassUsingParameter() {

        List<String> list = new ArrayList<>();
        Sources.register("input", Source.from("a", "b", "c"));
        Sinks.register("output", Sink.<String>forEach(list::add));

        Fluid fluid = new Fluid();
        assertThat(fluid.vertx()).isNotNull();

        fluid.deploy(MyMediator6.class);

        await().until(() -> list.size() == 3);
        assertThat(list).containsExactly("A", "B", "C");
    }

    @Test
    public void testDeploymentWithClassUsingParameterWithUnknown() {

        Sources.register("input", Source.from("a", "b", "c"));
        Sinks.unregister("output");

        Fluid fluid = new Fluid();
        assertThat(fluid.vertx()).isNotNull();

        try {
            fluid.deploy(MyMediator7.class);
            fail("Invalid configuration not detected");
        } catch (IllegalArgumentException e) {
            // OK
        }

        Sources.unregister("input");
        Sinks.register("output", Sink.discard());

        try {
            fluid.deploy(MyMediator8.class);
            fail("Invalid configuration not detected");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void testDeploymentWithClassUsingParameterWithBadParameter() {

        Sources.register("input", Source.from("a", "b", "c"));
        Sinks.register("output", Sink.discard());

        Fluid fluid = new Fluid();
        assertThat(fluid.vertx()).isNotNull();

        try {
            fluid.deploy(MyMediator9.class);
            fail("Invalid configuration not detected");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    static class MyMediator1 {

        @Port("input")
        Source<String> source;

        @Port("output")
        Sink<String> sink;

        @Transformation
        void transform() {
            source.transform(String::toUpperCase).to(sink);
        }

    }

    static class MyMediator2 {

        @Port("input")
        private Source<String> source;

        @Port("output")
        private Sink<String> sink;

        @Transformation
        private void transform() {
            source.transform(String::toUpperCase).to(sink);
        }

    }

    static class MyMediator3 {

        @Port("input")
        Source<String> source;

        @Port("output")
        Sink<String> sink;

    }

    static class MyMediator4 {

        @Port("missing-input")
        Source<String> source;

        @Port("output")
        Sink<String> sink;

        @Transformation
        void transform() {
            source.transform(String::toUpperCase).to(sink);
        }

    }

    static class MyMediator5 {

        @Port("input")
        Source<String> source;

        @Port("missing-output")
        Sink<String> sink;

        @Transformation
        void transform() {
            source.transform(String::toUpperCase).to(sink);
        }

    }

    static class MyMediator6 {
        
        @Transformation
        void transform(@Port("input")
                           Source<String> source,
                       @Port("output")
                           Sink<String> sink) {
            source.transform(String::toUpperCase).to(sink);
        }

    }

    static class MyMediator7 {

        @Transformation
        void transform(@Port("input")
                           Source<String> source,
                       @Port("missing-output")
                           Sink<String> sink) {
            source.transform(String::toUpperCase).to(sink);
        }

    }

    static class MyMediator8 {

        @Transformation
        void transform(@Port("missing-input")
                           Source<String> source,
                       @Port("output")
                           Sink<String> sink) {
            source.transform(String::toUpperCase).to(sink);
        }

    }

    static class MyMediator9 {

        @Port("input")
        Source<String> source;

        @Transformation
        void transform(@Port("output")
                           Sink<String> sink,
                       String foo) {
            source.transform(String::toUpperCase).transform(s -> foo).to(sink);
        }

    }

}