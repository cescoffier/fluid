package me.escoffier.fluid.examples.review;

import hu.akarnokd.rxjava2.math.MathFlowable;
import io.reactivex.Flowable;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Port;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.constructs.Pair;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;

import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ReviewRecentRating {

  @Port("reviews")
  Source<JsonObject> reviews;


  @Transformation
  public void computeStatistics() {
    reviews
      .transformPayloadFlow(flow ->
        flow
          .groupBy(data -> data.getString("course"))
          .flatMap(group ->
            group
              .map(i -> i.getInteger("rating"))
              .buffer(1, TimeUnit.MINUTES)
              .map(Flowable::fromIterable)
              .flatMap(MathFlowable::averageDouble)
              .map(avg -> Pair.pair(group.getKey(), avg)
              ))
      )
      .to(Sink.forEachPayload(pair -> System.out.println("Window rating of " + pair.left() + " : " + pair.right())));
  }

}
