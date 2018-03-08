package me.escoffier.fluid.example;

import hu.akarnokd.rxjava2.math.MathFlowable;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Mediator {

  @Transformation
  public void mediation(@Inbound("sensor") Source<JsonObject> input, @Inbound("eb-average") Sink<Double> output) {
    input
      .mapItem(json -> json.getDouble("data"))
      .composeItemFlowable(flow ->
        flow.window(5)
          .flatMap(MathFlowable::averageDouble)
      )
      .to(output);
  }

}
