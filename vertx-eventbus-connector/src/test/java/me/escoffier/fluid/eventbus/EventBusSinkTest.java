package me.escoffier.fluid.eventbus;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.Message;
import me.escoffier.fluid.models.Source;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSinkTest {


  private Vertx vertx;

  @Before
  public void setup() {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  @Test
  public void testSinkWithInteger() throws InterruptedException {
    String topic = UUID.randomUUID().toString();
    CountDownLatch latch = new CountDownLatch(1);
    List<Integer> list = new ArrayList<>();
    vertx.eventBus().consumer(topic).toFlowable().map(Message::body).cast(Integer.class).subscribe(i -> {
      list.add(i);
      if (list.size() == 10) {
        latch.countDown();
      }
    });


    EventBusSink<Integer> sink = new EventBusSink<>(vertx,
      new JsonObject()
        .put("address", topic)
    );


    Source.from(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      .mapPayload(i -> i + 1)
      .to(sink);

    latch.await();
    assertThat(list).containsExactly(2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
  }

  @Test
  public void testSinkWithString() throws InterruptedException {
    String topic = UUID.randomUUID().toString();
    CountDownLatch latch = new CountDownLatch(1);
    List<String> list = new ArrayList<>();
    vertx.eventBus().consumer(topic).toFlowable().map(Message::body).cast(String.class).subscribe(i -> {
      list.add(i);
      if (list.size() == 10) {
        latch.countDown();
      }
    });

    EventBusSink<String> sink = new EventBusSink<>(vertx,
      new JsonObject()
        .put("address", topic)
    );


    Stream<String> stream = new Random().longs(10).mapToObj(Long::toString);
    Source.fromPayloads(stream)
      .to(sink);

    latch.await();
    assertThat(list).hasSize(10);

  }

}
