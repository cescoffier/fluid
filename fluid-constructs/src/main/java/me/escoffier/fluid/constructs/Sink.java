package me.escoffier.fluid.constructs;

import io.reactivex.Completable;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents a data sink.
 */
public interface Sink<OUT> {

  Completable dispatch(OUT data);

  default String name() {
    return null;
  }

  /**
   * Transforms the current Sink into another Sink that transforms each incoming item before
   * calling current Sink. In other words, it creates a new Sink receiving data. Each data is
   * processed using the given function and the result is passed to the current Sink.
   * <p>
   * Notice that if the function return {@code null}, the data is ignored.
   *
   * @param function the function transforming the incoming data
   * @param <X>      the type of data received by the resulting sink
   * @return the new sink
   */
  default <X> Sink<X> contramap(Function<X, OUT> function) {
    return data -> {
      try {
        OUT processed = function.apply(data);
        if (processed != null) {
          return Sink.this.dispatch(processed);
        } else {
          // Data ignored.
          return Completable.complete();
        }
      } catch (Exception e) {
        return Completable.error(e);
      }
    };
  }

  static <T> Sink<T> forEach(Consumer<T> consumer) {
    return data -> Completable.fromAction(() -> consumer.accept(data));
  }

  /**
   * A sink discarding all inputs.
   *
   * @param <T> the excepted data type
   * @return the sink
   */
  static <T> Sink<T> discard() {
    return x -> Completable.fromAction(() -> {
    });
  }

  static <T> Sink<T> forEachAsync(Function<T, Completable> fun) {
    return fun::apply;
  }

  static <OUT, RES> ScanSink<OUT, RES> fold(RES init, BiFunction<OUT, RES, RES> mapper) {
    return new ScanSink<>(Objects.requireNonNull(init), Objects.requireNonNull(mapper));
  }

  static <T> HeadSink<T> head() {
    return new HeadSink<>();
  }

  static <T> TailSink<T> tail() {
    return new TailSink<>();
  }

  class ScanSink<OUT, RES> implements Sink<OUT> {
    private final BiFunction<OUT, RES, RES> mapper;
    private RES current;

    // TODO provide a flowable to collect the "current" values.

    ScanSink(RES init, BiFunction<OUT, RES, RES> mapper) {
      this.mapper = mapper;
      this.current = init;
    }

    @Override
    public synchronized Completable dispatch(OUT data) {
      return Completable.fromAction(() -> {
        synchronized (ScanSink.this) {
          current = mapper.apply(data, current);
        }
      });

    }

    RES value() {
      return current;
    }
  }

  class HeadSink<OUT> implements Sink<OUT> {
    private volatile OUT head;

    @Override
    public Completable dispatch(OUT data) {
      return Completable.fromAction(() -> {
        synchronized (HeadSink.this) {
          if (head == null) {
            // TODO It would be nice to be able to cancel the subscription
            head = data;
          }
        }
      });
    }

    synchronized OUT value() {
      return head;
    }
  }

  class TailSink<OUT> implements Sink<OUT> {
    private volatile OUT tail;

    @Override
    public Completable dispatch(OUT data) {
      return Completable.fromAction(() -> {
        synchronized (TailSink.this) {
          tail = data;
        }
      });
    }

    synchronized OUT value() {
      return tail;
    }
  }

}
