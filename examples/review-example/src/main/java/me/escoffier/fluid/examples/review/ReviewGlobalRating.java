package me.escoffier.fluid.examples.review;

import io.reactivex.functions.BiFunction;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Port;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.constructs.*;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ReviewGlobalRating {

  @Port("reviews")
  Source<JsonObject> reviews;


  @Transformation
  public void computeStatistics() {
    reviews
      .transformFlow(flow ->
        flow
          .groupBy(data -> data.item().getString("course"))
          .flatMap(group ->
            group.map(Data::item)
              .map(i -> i.getInteger("rating"))
              .scan(Tuple.tuple(0L, 0.0, 0.0), average())
              .map(tuple -> Pair.pair(group.getKey(), (double) tuple.nth(2)))
              .map(Data::new)
          )
      )
      .to(Sink.forEachItem(pair -> System.out.println("Rating of " + pair.left() + " : " + pair.right())));
  }

  /**
   * Function computing an average of {@link Integer}.
   * It returns a {@link Tuple} structured as follows: 0: element-count, 1: sum of the element, 2: average
   * @return a tuple containing in this order the number of element in the series, the sum of the element and the
   * average.
   */
  public static BiFunction<Tuple, Integer, Tuple> average() {
    return (tuple, rating) -> {
      long count = tuple.nth(0);
      double sum = tuple.nth(1);

      count = count + 1;
      sum = sum + rating;
      double avg = sum / count;

      return Tuple.tuple(count, sum, avg);
    };
  }

  private boolean isFraud(Data<JsonObject> data) {
    return Math.random() > 0.80;
  }

}
