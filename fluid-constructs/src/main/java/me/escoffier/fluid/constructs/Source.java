package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import me.escoffier.fluid.constructs.impl.SourceImpl;
import org.reactivestreams.Publisher;

import java.util.Objects;

/**
 * Represents a data source. It emits {@link Data<T>}.
 */
public interface Source<T> extends DataStream<T> {

  default Source<T> orElse(Source<T> alt) {
    return
      new SourceImpl<>(this.flow().switchIfEmpty(alt.flow()));
  }

  static <T> Source<T> from(Publisher<Data<T>> flow) {
    return from(Flowable.fromPublisher(Objects.requireNonNull(flow)));
  }

  static <T> Source<T> fromItems(Publisher<T> flow) {
    return fromItems(Flowable.fromPublisher(Objects.requireNonNull(flow)));
  }

  static <T> Source<T> from(Flowable<Data<T>> flow) {
    return new SourceImpl<>(Objects.requireNonNull(flow));
  }

  static <T> Source<T> fromItems(Flowable<T> flow) {
    return new SourceImpl<>(Objects.requireNonNull(flow).map(Data::new));
  }

  static <T> Source<T> from(Single<Data<T>> single) {
    return from(Objects.requireNonNull(single).toFlowable());
  }

  static <T> Source<T> fromItem(Single<T> single) {
    return fromItems(Objects.requireNonNull(single).toFlowable());
  }

  static <T> Source<T> from(Maybe<Data<T>> maybe) {
    return new SourceImpl<>(Objects.requireNonNull(maybe).toFlowable());
  }

  static <T> Source<T> fromItem(Maybe<T> maybe) {
    return fromItems(Objects.requireNonNull(maybe).toFlowable());
  }

  static <T> Source<T> from(Data<T>... items) {
    return from(Flowable.fromArray(Objects.requireNonNull(items)));
  }

  static <T> Source<T> from(T... items) {
    return fromItems(Flowable.fromArray(Objects.requireNonNull(items)));
  }

  static <T> Source<T> from(Iterable<Data<T>> items) {
    return from(Flowable.fromIterable(Objects.requireNonNull(items)));
  }

  static <T> Source<T> fromItems(Iterable<T> items) {
    return fromItems(Flowable.fromIterable(Objects.requireNonNull(items)));
  }

  static <T> Source<T> from(java.util.stream.Stream<Data<T>> stream) {
    return from(Flowable.fromIterable(stream::iterator));
  }

  static <T> Source<T> fromItems(java.util.stream.Stream<T> stream) {
    return fromItems(Flowable.fromIterable(stream::iterator));
  }

  static <T> Source<T> empty() {
    return from(Flowable.empty());
  }

  static <T> Source<T> just(T item) {
    return fromItems(Flowable.just(item));
  }

  static <T> Source<T> just(Data<T> item) {
    return from(Flowable.just(item));
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
