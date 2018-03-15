package me.escoffier.fluid.example;

import io.reactivex.Flowable;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Outbound;
import me.escoffier.fluid.annotations.Transformation;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreams;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultMessage;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class CamelComponent {

  @Transformation
  @Outbound("result")
  public Publisher<Integer> mediation(@Inbound("sensor") Flowable<JsonObject> input) throws Exception {
    CamelContext context = new DefaultCamelContext();
    CamelReactiveStreamsService camel = CamelReactiveStreams.get(context);

    Subscriber<JsonObject> elements = camel.streamSubscriber("elements", JsonObject.class);

    Flowable<Integer> flowable = Flowable.fromPublisher(camel.fromStream("out"))
      .map(exchange -> (Integer) exchange.getIn().getBody());

    context.addRoutes(new RouteBuilder() {
      @Override
      public void configure() {
        from("reactive-streams:elements")
          .setBody().body(json -> ((JsonObject)json).getInteger("data"))
          .to("reactive-streams:out");
      }
    });

    input.subscribe(elements);

    context.start();

    return flowable;
  }

}
