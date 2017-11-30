package me.escoffier.fluid.examples.review;

import io.reactivex.Flowable;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Port;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ReviewProducer {

  @Port("reviews")
  Sink<JsonObject> sink;

  private static final Logger logger = LogManager.getLogger(ReviewProducer.class);

  private int reviewCount = 0;
  private Random random = new Random();

  @Transformation
  public void generateReviews() {
    Source.fromItems(Flowable.interval(1, TimeUnit.SECONDS))
      .transform(x -> {
        reviewCount += 1;
        JsonObject review = new JsonObject()
          .put("course", pickACourse())
          .put("rating", rate());
        return new Data<>(review).with("count", reviewCount);
      })
      .onData(data ->
        logger.info("Sending review " + data.get("count") + ": " + data.item().encode()))
      .to(sink);
  }

  private int rate() {
    return random.nextInt(5) + 1;
  }

  private String pickACourse() {
    Collections.shuffle(courses);
    return courses.get(0);
  }

  private static List<String> courses = Arrays.asList(
    "Dead Learning 101",
    "Python for nobody",
    "Statistics with D",
    "Convoluted Neural Network",
    "Introduction to psychology"
  );

}
