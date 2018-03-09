package me.escoffier.fluid.eventbus;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Check the behavior of the event bus source.
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSourceTest {

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
  public void testSource() {
    String topic = UUID.randomUUID().toString();

    EventBusSource<Integer> source = new EventBusSource<>(vertx,
      new JsonObject()
        .put("address", topic)
    );

    List<Integer> results = new ArrayList<>();
    source
      .mapPayload(i -> i + 1)
      .to(Sink.forEachPayload(results::add));

    AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      vertx.eventBus().send(topic, counter.getAndIncrement());
    }

    await().atMost(1, TimeUnit.MINUTES).until(() -> results.size() >= 10);
    assertThat(results).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

  }

  @Test
  public void testMulticastWithBufferSize() throws InterruptedException {
    String topic = UUID.randomUUID().toString();

    EventBusSource<Integer> source = new EventBusSource<>(vertx,
      new JsonObject()
        .put("address", topic)
        .put("multicast.buffer.size", 20)
    );

    checkMulticast(topic, source);

  }

  private void checkMulticast(String topic, Source<Integer> source) {
    List<Integer> resultsA = new ArrayList<>();
    List<Integer> resultsB = new ArrayList<>();
    source
      .mapPayload(i -> i + 1)
      .to(Sink.forEachPayload(resultsB::add));

    source
      .mapPayload(i -> i + 1)
      .to(Sink.forEachPayload(resultsA::add));

    AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      vertx.eventBus().send(topic, counter.getAndIncrement());
    }

    await().atMost(1, TimeUnit.MINUTES).until(() -> resultsA.size() >= 10);
    await().atMost(1, TimeUnit.MINUTES).until(() -> resultsB.size() >= 10);
    assertThat(resultsA).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    assertThat(resultsB).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  @Test
  public void testMulticastWithTime() {
    String topic = UUID.randomUUID().toString();

    EventBusSource<Integer> source = new EventBusSource<>(vertx,
      new JsonObject()
        .put("address", topic)
        .put("multicast.buffer.period.ms", 2000)
    );

    checkMulticast(topic, source);

  }


}
