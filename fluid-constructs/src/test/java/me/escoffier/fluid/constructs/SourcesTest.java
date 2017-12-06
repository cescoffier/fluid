package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static me.escoffier.fluid.constructs.FlowContext.window;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests the common behavior of sources.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SourcesTest {

  @Test
  public void testMultiCast() {
    Source<Integer> source = Source.fromPayloads(Flowable.range(1, 10)
      .replay().autoConnect());
    ListSink<Integer> sink1 = Sink.list();
    ListSink<Integer> sink2 = Sink.list();

    source.to(sink1);
    source.to(sink2);

    assertThat(sink1.values()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    assertThat(sink2.values()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  @Test
  public void testWindowBySize() {
    ListSink<String> sink = Sink.list();
    Set<List<Data<String>>> windows = new HashSet<>();
    Flowable<String> items = Flowable.fromArray("a", "b", "c", "d", "e", "f", "g");
    Source.fromPayloads(items).windowBySize(2)
      .transformPayload(String::toUpperCase)
      .onData(data -> windows.add(window()))
      .to(sink);

    await().until(() -> sink.values().size() >= 6);
    assertThat(sink.values()).containsExactly("A", "B", "C", "D", "E", "F", "G");
    assertThat(windows).hasSize(4);
  }

  @Test
  public void testWindowsByTime() {
    ListSink<String> sink = Sink.list();
    Set<List<Data<String>>> windows = new HashSet<>();
    Flowable<String> items = Flowable.fromArray("a", "b", "c", "d", "e", "f", "g")
      .zipWith(Flowable.interval(10, TimeUnit.MILLISECONDS), (s, l) -> s);

    Source.fromPayloads(items).windowByTime(20, TimeUnit.MILLISECONDS)
      .transformPayload(String::toUpperCase)
      .onData(data -> windows.add(window()))
      .to(sink);

    await().until(() -> sink.values().size() >= 6);
    assertThat(sink.values()).containsExactly("A", "B", "C", "D", "E", "F", "G");
    assertThat(windows).hasSize(4);
  }

  @Test
  public void testFromMaybe() {
    Maybe<String> empty = Maybe.empty();
    Maybe<String> full = Maybe.just("hello");

    ListSink<String> sink = Sink.list();
    Source.fromPayload(empty)
      .to(sink);

    assertThat(sink.data()).isEmpty();

    Source.fromPayload(full)
      .to(sink);

    assertThat(sink.values()).containsExactly("hello");
  }

  @Test
  public void testFromSingle() {
    Single<String> value = Single.just("hello");

    ListSink<String> sink = Sink.list();
    Source.fromPayload(value)
      .to(sink);

    assertThat(sink.values()).containsExactly("hello");
  }

  @Test
  public void testOrElse() {
    Flowable<String> empty = Flowable.empty();
    Flowable<String> other = Flowable.fromArray("a", "b", "c");
    Flowable<String> another = Flowable.fromArray("e", "f", "g");

    ListSink<String> sink = Sink.list();
    Source.fromPayloads(empty).orElse(Source.fromPayloads(other))
      .to(sink);
    assertThat(sink.values()).containsExactly("a", "b", "c");

    ListSink<String> sink2 = Sink.list();
    Source.fromPayloads(other).orElse(Source.fromPayloads(another)).to(sink2);
    assertThat(sink.values()).containsExactly("a", "b", "c");
  }


}
