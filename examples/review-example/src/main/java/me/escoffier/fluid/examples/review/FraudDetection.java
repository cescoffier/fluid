package me.escoffier.fluid.examples.review;

import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Port;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.constructs.Source;

import java.util.Random;

import static me.escoffier.fluid.constructs.Source.from;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FraudDetection {

  @Port("reviews")
  Source<JsonObject> reviews;

  @Transformation
  public void detectFraud() {
    from(reviews)
      .branch(

      )

  }

}
