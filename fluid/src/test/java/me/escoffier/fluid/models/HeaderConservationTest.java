package me.escoffier.fluid.models;

import io.reactivex.Flowable;
import me.escoffier.fluid.impl.ListSink;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class HeaderConservationTest {

  private Random random = new Random();
  private AtomicInteger counter = new AtomicInteger();

  private Supplier<Data<Double>> producer = () ->
    new Data<>(random.nextDouble()).with("index", counter.incrementAndGet()).with("timestamp", System.currentTimeMillis());

  @Test
  public void testConservationWhenTransformingPayload() {
    Flowable<Data<Double>> flowable = Flowable.fromArray(producer.get(), producer.get(), producer.get(), producer.get(), producer.get());
    ListSink<String> sink = Sink.list();
    Source.from(flowable)
      .mapItem(d -> "foo")
      .to(sink);

    assertThat(sink.values()).hasSize(5);
    for (Data<String> d : sink.data()) {
      assertThat((long) d.get("timestamp")).isNotNull().isNotZero().isNotNegative();
      assertThat((int) d.get("index")).isNotNull().isNotZero().isNotNegative();
      assertThat(d.payload()).isEqualToIgnoringCase("foo");
    }
  }

  @Test
  public void testConservationWhenTransformingPayloadStream() {
    Flowable<Data<Double>> flowable = Flowable.fromArray(producer.get(), producer.get(), producer.get(), producer.get(), producer.get());
    ListSink<String> sink = Sink.list();
    Source.from(flowable)
      .composeItemFlowable(s -> s.map(x -> {
          if (random.nextBoolean()) {
            return "foo";
          }
          return "bar";
        }).map(String::toUpperCase)
      )
      .to(sink);

    assertThat(sink.values()).hasSize(5);
    for (Data<String> d : sink.data()) {
      assertThat((long) d.get("timestamp")).isNotNull().isNotZero().isNotNegative();
      assertThat((int) d.get("index")).isNotNull().isNotZero().isNotNegative();
      assertThat(d.payload()).isIn("FOO", "BAR");
    }
  }

  @Test
  public void testConservationWhenTransformingPayloadStreamWithDoubleEmission() {
    Flowable<Data<Double>> flowable = Flowable.fromArray(producer.get(), producer.get(), producer.get(), producer.get(), producer.get());
    ListSink<String> sink = Sink.list();
    Source.from(flowable)
      .composeItemFlowable(s -> s
        .map(x -> {
          if (random.nextBoolean()) {
            return "foo";
          }
          return "bar";
        })
        .flatMap(v -> Flowable.just(v.toUpperCase(), v.toUpperCase()))
      )
      .to(sink);

    assertThat(sink.values()).hasSize(10);
    int i = 1;
    long lastTimestamp = 0;
    int lastIndex = 0;
    for (Data<String> d : sink.data()) {
      assertThat((long) d.get("timestamp")).isNotNull().isNotZero().isNotNegative();
      assertThat((int) d.get("index")).isNotNull().isNotZero().isNotNegative();
      assertThat(d.payload()).isIn("FOO", "BAR");
      if (i % 2 == 0) {
        assertThat((long) d.get("timestamp")).isEqualTo(lastTimestamp);
        assertThat((int) d.get("index")).isEqualTo(lastIndex);
      } else {
        lastTimestamp = d.get("timestamp");
        lastIndex = d.get("index");
      }
      i++;
    }
  }
}
