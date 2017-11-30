package me.escoffier.fluid.example;

import hu.akarnokd.rxjava2.math.MathFlowable;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Port;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Mediator {

  @Transformation
  public void mediation(@Port("sensor") Source<JsonObject> input, @Port("eb-average") Sink<Double> output) {
    input
      .transformPayload(json -> json.getDouble("data"))
      .transformPayloadFlow(flow ->
        flow.window(5)
          .flatMap(MathFlowable::averageDouble)
      )

      .to(output);
  }

}
