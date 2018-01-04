package me.escoffier.fluid.constructs;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Completable;
import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

/**
 * Checks the Sink behavior.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SinkTest {


  @Test
  public void testDiscard() {
    Sink<Integer> sink = Sink.discard();
    assertThat(sink.name()).isNull();
    Completable c1 = sink.dispatch(1);
    Completable c2 = sink.dispatch(2);
    Completable c3 = sink.dispatch(3);

    assertThat(c1.blockingGet()).isNull();
    assertThat(c2.blockingGet()).isNull();
    assertThat(c3.blockingGet()).isNull();
  }

  @Test
  public void testHead() {
    HeadSink<Integer> sink = Sink.head();
    assertThat(sink.name()).isNull();
    Completable c1 = sink.dispatch(1);
    Completable c2 = sink.dispatch(2);
    Completable c3 = sink.dispatch(3);

    assertThat(c1.blockingGet()).isNull();
    assertThat(c2.blockingGet()).isNull();
    assertThat(c3.blockingGet()).isNull();

    assertThat(sink.value()).isEqualTo(1);
  }

  @Test
  public void testTail() {
    TailSink<Integer> sink = Sink.tail();
    assertThat(sink.name()).isNull();
    Completable c1 = sink.dispatch(1);
    Completable c2 = sink.dispatch(2);
    Completable c3 = sink.dispatch(3);

    assertThat(c1.blockingGet()).isNull();
    assertThat(c2.blockingGet()).isNull();
    assertThat(c3.blockingGet()).isNull();

    assertThat(sink.value()).isEqualTo(3);
  }

  @Test
  public void testForEachPayload() {
    List<Integer> list = new ArrayList<>();
    Sink<Integer> sink = Sink.forEachPayload(list::add);
    assertThat(sink.name()).isNull();
    Completable c1 = sink.dispatch(1);
    Completable c2 = sink.dispatch(2);
    Completable c3 = sink.dispatch(3);

    assertThat(c1.blockingGet()).isNull();
    assertThat(c2.blockingGet()).isNull();
    assertThat(c3.blockingGet()).isNull();

    assertThat(list).containsExactly(1, 2, 3);
  }

  @Test
  public void testForEachPayloadOnWatermark() {
    List<Integer> list = new ArrayList<>();
    Sink<Integer> sink = Sink.forEachPayload(list::add, true);

    AtomicReference<Emitter<Integer>> reference = new AtomicReference<>();
    Flowable<Integer> flowable = Flowable.create(reference::set, BackpressureStrategy.BUFFER);

    Source.fromPayloads(flowable).withWindow(Windowing.bySize(2)).to(sink);
    assertThat(sink.name()).isNull();

    assertThat(list).isEmpty();
    reference.get().onNext(1);
    assertThat(list).isEmpty();
    reference.get().onNext(2);
    reference.get().onNext(3);
    assertThat(list).containsExactly(1, 2);
    reference.get().onNext(4);
    reference.get().onNext(5);
    assertThat(list).containsExactly(1, 2, 3, 4);
    reference.get().onComplete();
    assertThat(list).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  public void testForEach() {
    List<Data<Integer>> list = new ArrayList<>();
    Sink<Integer> sink = Sink.forEach(list::add);
    assertThat(sink.name()).isNull();
    Completable c1 = sink.dispatch(1);
    Completable c2 = sink.dispatch(2);
    Completable c3 = sink.dispatch(3);

    assertThat(c1.blockingGet()).isNull();
    assertThat(c2.blockingGet()).isNull();
    assertThat(c3.blockingGet()).isNull();

    assertThat(list.stream().map(Data::payload).collect(Collectors.toList()))
      .containsExactly(1, 2, 3);
  }

  @Test
  public void testForEachDataOnWatermark() {
    List<Integer> list = new ArrayList<>();
    Sink<Integer> sink = Sink.forEach(d -> list.add(d.payload()), true);

    AtomicReference<Emitter<Integer>> reference = new AtomicReference<>();
    Flowable<Integer> flowable = Flowable.create(reference::set, BackpressureStrategy.BUFFER);

    Source.fromPayloads(flowable).withWindow(Windowing.bySize(2)).to(sink);
    assertThat(sink.name()).isNull();

    assertThat(list).isEmpty();
    reference.get().onNext(1);
    assertThat(list).isEmpty();
    reference.get().onNext(2);
    reference.get().onNext(3);
    assertThat(list).containsExactly(1, 2);
    reference.get().onNext(4);
    reference.get().onNext(5);
    assertThat(list).containsExactly(1, 2, 3, 4);
    reference.get().onComplete();
    assertThat(list).containsExactly(1, 2, 3, 4, 5);
  }


  @Test
  public void testForEachAsync() {
    List<Data<Integer>> list = new ArrayList<>();
    Sink<Integer> sink = Sink.forEachAsync(i -> {
      list.add(i);
      return Completable.complete();
    });
    assertThat(sink.name()).isNull();
    Completable c1 = sink.dispatch(1);
    Completable c2 = sink.dispatch(2);
    Completable c3 = sink.dispatch(3);

    assertThat(c1.blockingGet()).isNull();
    assertThat(c2.blockingGet()).isNull();
    assertThat(c3.blockingGet()).isNull();

    assertThat(list.stream().map(Data::payload).collect(Collectors.toList()))
      .containsExactly(1, 2, 3);
  }

  @Test
  public void testForEachAsyncOnWatermark() {
    List<Integer> list = new ArrayList<>();
    Sink<Integer> sink = Sink.forEachAsync(i -> {
      list.add(i.payload());
      return Completable.complete();
    }, true);

    AtomicReference<Emitter<Integer>> reference = new AtomicReference<>();
    Flowable<Integer> flowable = Flowable.create(reference::set, BackpressureStrategy.BUFFER);

    Source.fromPayloads(flowable).withWindow(Windowing.bySize(2)).to(sink);
    assertThat(sink.name()).isNull();

    assertThat(list).isEmpty();
    reference.get().onNext(1);
    assertThat(list).isEmpty();
    reference.get().onNext(2);
    reference.get().onNext(3);
    assertThat(list).containsExactly(1, 2);
    reference.get().onNext(4);
    reference.get().onNext(5);
    assertThat(list).containsExactly(1, 2, 3, 4);
    reference.get().onComplete();
    assertThat(list).containsExactly(1, 2, 3, 4, 5);
  }


  @Test
  public void testForEachWithFailure() {
    List<Integer> list = new ArrayList<>();
    Sink<Integer> sink = Sink.forEachPayload(i -> {
      if (i == 2) {
        throw new IllegalArgumentException("No no no");
      } else {
        list.add(i);
      }
    });
    assertThat(sink.name()).isNull();
    Completable c1 = sink.dispatch(1);
    Completable c2 = sink.dispatch(2);
    Completable c3 = sink.dispatch(3);

    assertThat(c1.blockingGet()).isNull();
    assertThat(c2.blockingGet()).isInstanceOf(IllegalArgumentException.class);
    assertThat(c3.blockingGet()).isNull();

    assertThat(list).containsExactly(1, 3);
  }

  @Test
  public void testBackPressure() {
    AtomicInteger counter = new AtomicInteger();
    Publisher<Integer> publisher = Flowable.range(0, 5000);

    Sink<Integer> slow = data -> Completable.fromAction(() -> {
      Thread.sleep(10);
      counter.incrementAndGet();
    });

    Source.fromPayloads(publisher)
      .transformFlow(f -> f
        .observeOn(Schedulers.computation()))
      .to(slow);

    await()
      .atMost(1, TimeUnit.MINUTES)
      .untilAtomic(counter, is(greaterThan(4000)));

    assertThat(counter.doubleValue()).isGreaterThan(4000.0);
  }

  @Test
  public void testContraMap() {
    CacheSink<String> cache = new CacheSink<>();

    Sink<Integer> sink = cache.contramap(i -> {
      if (i == 3) {
        return null;
      }
      return new Data<>(i.toString());
    });
    assertThat(sink.dispatch(1).blockingGet()).isNull();
    assertThat(sink.dispatch(2).blockingGet()).isNull();
    assertThat(sink.dispatch(3).blockingGet()).isNull();
    assertThat(sink.dispatch(4).blockingGet()).isNull();

    assertThat(cache.buffer).containsExactly("1", "2", "4");
  }

  @Test
  public void testFold() {
    ScanSink<Integer, Integer> sink = Sink.fold(0, (l, i) -> l + i);
    assertThat(sink.dispatch(1).blockingGet()).isNull();
    assertThat(sink.dispatch(2).blockingGet()).isNull();
    assertThat(sink.dispatch(3).blockingGet()).isNull();
    assertThat(sink.dispatch(4).blockingGet()).isNull();

    assertThat(sink.value()).isEqualTo(10);
  }

  @Test
  public void testListSink() {
    ListSink<Object> list = Sink.list();
    Sink<Integer> sink = list.contramap(i -> {
      if (i == 3) {
        return null;
      }
      return new Data<>(i.toString());
    });
    assertThat(sink.dispatch(1).blockingGet()).isNull();
    assertThat(sink.dispatch(2).blockingGet()).isNull();
    assertThat(sink.dispatch(3).blockingGet()).isNull();
    assertThat(sink.dispatch(4).blockingGet()).isNull();

    assertThat(list.values()).containsExactly("1", "2", "4");
  }

}
