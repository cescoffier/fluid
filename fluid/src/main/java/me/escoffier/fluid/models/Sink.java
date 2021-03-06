package me.escoffier.fluid.models;

import io.reactivex.Completable;
import me.escoffier.fluid.impl.HeadSink;
import me.escoffier.fluid.impl.ListSink;
import me.escoffier.fluid.impl.ScanSink;
import me.escoffier.fluid.impl.TailSink;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents a data sink.
 * It receives {@link Message <OUT>}.
 */
public interface Sink<OUT> {

  Completable dispatch(Message<OUT> message);

  default Completable dispatch(OUT data) {
    return dispatch(new Message<>(data));
  }

  default String name() {
    return null;
  }

  /**
   * Transforms the current Sink into another Sink that transforms each incoming payload (encapsulated in the
   * {@link Message} before calling current Sink. In other words, it creates a new Sink receiving data. Each data is
   * processed using the given function and the result is passed to the current Sink.
   * <p>
   * Notice that if the function return {@code null}, the data is ignored.
   *
   * @param function the function transforming the incoming data
   * @param <X>      the type of data received by the resulting sink
   * @return the new sink
   */
  default <X> Sink<X> contramap(Function<X, Message<OUT>> function) {
    return data -> {
      try {
        Message<OUT> processed = function.apply(data.payload());
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

  static <T> Sink<T> forEach(Consumer<Message<T>> consumer) {
    // TODO here we could detect if the consumer wants data or just the payload.
    return data -> Completable.fromAction(() -> consumer.accept(data));
  }

  static <T> Sink<T> forEachPayload(Consumer<T> consumer) {
    return data -> Completable.fromAction(() -> consumer.accept(data.payload()));
  }

  static <T> ListSink<T> list() {
    return new ListSink<>();
  }

  /**
   * A sink discarding all inputs.
   *
   * @param <T> the excepted data type
   * @return the sink
   */
  static <T> Sink<T> discard() {
    return x -> Completable.complete();
  }

  static <T> Sink<T> forEachAsync(Function<Message<T>, Completable> fun) {
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

}
