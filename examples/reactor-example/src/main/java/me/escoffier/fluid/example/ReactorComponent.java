package me.escoffier.fluid.example;

import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Outbound;
import me.escoffier.fluid.annotations.Transformation;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ReactorComponent {

  @Transformation
  @Outbound("result")
  public Publisher<Integer> mediation(@Inbound("sensor") Publisher<JsonObject> input) {

    return Flux.from(input)
      .map(json -> json.getInteger("data"));
  }

}
