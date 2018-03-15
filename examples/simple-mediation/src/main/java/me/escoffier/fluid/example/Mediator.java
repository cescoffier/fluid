package me.escoffier.fluid.example;

import hu.akarnokd.rxjava2.math.MathFlowable;
import io.reactivex.Flowable;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Outbound;
import me.escoffier.fluid.annotations.Transformation;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Mediator {

  @Transformation
  @Outbound("eb-average")
  public Flowable<Double> mediation(@Inbound("sensor") Flowable<JsonObject> input) {
    return input
      .map(json -> json.getDouble("data"))
      .window(5)
      .flatMap(MathFlowable::averageDouble)
      .doOnNext(d -> System.out.println("Produced Average: " + d));
  }

}
