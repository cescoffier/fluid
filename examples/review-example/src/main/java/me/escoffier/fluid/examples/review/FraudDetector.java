package me.escoffier.fluid.examples.review;

import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Port;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.models.Data;
import me.escoffier.fluid.models.Pair;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FraudDetector {

  @Port("movies")
  Source<JsonObject> reviews;

  @Port("reviews")
  Sink<JsonObject> valid;

  @Port("fraud")
  Sink<JsonObject> fraud;

  @Transformation
  public void detect() {
    Pair<Source<JsonObject>, Source<JsonObject>> streams = reviews
      .branch(this::isFraud);

    streams.left().to(fraud);
    streams.right().to(valid);

  }

  private boolean isFraud(Data<JsonObject> data) {
    return Math.random() > 0.80;
  }

}
