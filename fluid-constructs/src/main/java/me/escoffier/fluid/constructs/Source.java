package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import org.reactivestreams.Publisher;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Represents a data source. It emits {@link Data<T>}.
 */
public interface Source<T> extends DataStream<T> {

  default Source<T> orElse(Source<T> alt) {
    return
      new AbstractSource<>(this.flow().switchIfEmpty(alt.flow()));
  }

  static <T> Source<T> from(Publisher<Data<T>> flow) {
    return from(Flowable.fromPublisher(Objects.requireNonNull(flow)));
  }

  static <T> Source<T> fromPayloads(Publisher<T> flow) {
    return fromPayloads(Flowable.fromPublisher(Objects.requireNonNull(flow)));
  }

  static <T> Source<T> from(Flowable<Data<T>> flow) {
    return new AbstractSource<>(Objects.requireNonNull(flow));
  }

  static <T> Source<T> fromPayloads(Flowable<T> flow) {
    return new AbstractSource<>(Objects.requireNonNull(flow).map(Data::new));
  }

  static <T> Source<T> from(Single<Data<T>> single) {
    return from(Objects.requireNonNull(single).toFlowable());
  }

  static <T> Source<T> fromPayload(Single<T> single) {
    return fromPayloads(Objects.requireNonNull(single).toFlowable());
  }

  static <T> Source<T> from(Maybe<Data<T>> maybe) {
    return new AbstractSource<>(Objects.requireNonNull(maybe).toFlowable());
  }

  static <T> Source<T> fromPayload(Maybe<T> maybe) {
    return fromPayloads(Objects.requireNonNull(maybe).toFlowable());
  }

  static <T> Source<T> from(Data<T>... payloads) {
    return from(Flowable.fromArray(Objects.requireNonNull(payloads)));
  }

  static <T> Source<T> from(T... payloads) {
    return fromPayloads(Flowable.fromArray(Objects.requireNonNull(payloads)));
  }

  static <T> Source<T> from(Iterable<Data<T>> payloads) {
    return from(Flowable.fromIterable(Objects.requireNonNull(payloads)));
  }

  static <T> Source<T> fromPayloads(Iterable<T> payloads) {
    return fromPayloads(Flowable.fromIterable(Objects.requireNonNull(payloads)));
  }

  static <T> Source<T> from(java.util.stream.Stream<Data<T>> stream) {
    return from(Flowable.fromIterable(stream::iterator));
  }

  static <T> Source<T> fromPayloads(java.util.stream.Stream<T> stream) {
    return fromPayloads(Flowable.fromIterable(stream::iterator));
  }

  static <T> Source<T> empty() {
    return from(Flowable.empty());
  }

  static <T> Source<T> just(T payload) {
    return fromPayloads(Flowable.just(payload));
  }

  static <T> Source<T> just(Data<T> payload) {
    return from(Flowable.just(payload));
  }

  static <T> Source<T> failed() {
    return from(Flowable
      .error(new Exception("Source failure")));
  }

  static <T> Source<T> failed(Throwable t) {
    return
      from(Flowable.error(Objects.requireNonNull(t)));
  }

  default String name() {
    return null;
  }

}
