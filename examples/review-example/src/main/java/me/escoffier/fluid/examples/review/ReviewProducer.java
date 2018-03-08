package me.escoffier.fluid.examples.review;

import io.reactivex.Flowable;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.models.Data;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ReviewProducer {

  @Inbound("movies")
  Sink<JsonObject> sink;

  private int reviewCount = 0;
  private Random random = new Random();

  @Transformation
  public void generateReviews() {
    Source.fromPayloads(Flowable.interval(1, TimeUnit.SECONDS))
      .map(x -> {
        reviewCount += 1;
        JsonObject review = new JsonObject()
          .put("course", pickAMovie())
          .put("rating", rate())
          .put("review.id", reviewCount);
        return new Data<>(review).with("count", reviewCount);
      })
      .to(sink);
  }

  private int rate() {
    return random.nextInt(5) + 1;
  }

  private String pickAMovie() {
    Collections.shuffle(movies);
    return movies.get(0);
  }

  private final static List<String> movies = Arrays.asList(
    "The Shawshank Redemption",
    "The Godfather",
    "The Godfather: Part II",
    "The Dark Knight",
    "12 Angry Men",
    "Schindler's List",
    "Pulp Fiction",
    "The Lord of the Rings: The Return of the King",
    "The Good, the Bad and the Ugly",
    "Fight Club"
  );

}
