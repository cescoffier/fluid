package me.escoffier.fluid.camel.sink;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import me.escoffier.fluid.constructs.Source;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class CamelSinkTest {

    @Test
    public void shouldWrapIntegersIntoCamelBodies(TestContext context) throws Exception {
        Async async = context.async();
        CamelSink<Integer> sink = new CamelSink<>(
                new JsonObject().put("endpoint", "direct:test")
        );
        CamelContext camelContext = sink.camelContext();
        camelContext.addRoutes(new RouteBuilder() {

            @Override public void configure() throws Exception {
                from("direct:test").process(event -> {
                    if (event.getIn().getBody(Integer.class) == 10) {
                        async.complete();
                    }
                });
            }
        });

        Source.from(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).to(sink);
    }

}