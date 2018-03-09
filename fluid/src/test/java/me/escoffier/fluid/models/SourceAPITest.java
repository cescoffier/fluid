package me.escoffier.fluid.models;

import io.reactivex.Flowable;
import me.escoffier.fluid.impl.ListSink;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SourceAPITest {

  @Test
  public void testCompose() {
    ListSink<Integer> sink = new ListSink<>();

    Source.from(1, 2, 3, 4, 5)
      .compose(pub -> Flowable.fromPublisher(pub).map(Message::payload).map(d -> d + 1).map(Message::new))
      .to(sink);

    await().until(() -> sink.values().size() == 5);
    assertThat(sink.values()).containsExactly(2, 3, 4, 5, 6);
  }

  @Test
  public void testTransform() {
    ListSink<Integer> sink = new ListSink<>();

    Source.from(1, 2, 3, 4, 5)
      .mapItem(d -> d + 1)
      .to(sink);

    await().until(() -> sink.values().size() == 5);
    assertThat(sink.values()).containsExactly(2, 3, 4, 5, 6);
  }

  @Test
  public void testChainingTransformation() {
    ListSink<Integer> sink = new ListSink<>();

    java.util.stream.Stream<Integer> stream = java.util.stream.Stream.iterate(0, i -> i + 1)
      .skip(10)
      .limit(10);
    Source.fromPayloads(stream)
      .mapItem(x -> x + 1)
      .mapItem(i -> i * 2)
      .to(sink);

    await().until(() -> sink.values().size() >= 5);
    assertThat(sink.values()).containsExactly(22, 24, 26, 28, 30, 32, 34, 36, 38, 40);
  }

}
