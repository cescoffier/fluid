package me.escoffier.fluid.models;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Represents a data source. It emits {@link Data<T>}.
 */
public interface Source<T> extends Publisher<Data<T>> {

  Source<T> named(String name);

  Source<T> withAttribute(String key, Object value);

  static <T> Source<T> from(Publisher<Data<T>> flow) {
    return from(Flowable.fromPublisher(Objects.requireNonNull(flow)));
  }

  static <T> Source<T> fromPayloads(Publisher<T> flow) {
    return fromPayloads(Flowable.fromPublisher(Objects.requireNonNull(flow)));
  }

  static <T> Source<T> from(Flowable<Data<T>> flow) {
    return new AbstractSource<>(Objects.requireNonNull(flow), null, null);
  }

  static <T> Source<T> fromPayloads(Flowable<T> flow) {
    return new AbstractSource<>(Objects.requireNonNull(flow).map(Data::new), null, null);
  }

  static <T> Source<T> from(Single<Data<T>> single) {
    return from(Objects.requireNonNull(single).toFlowable());
  }

  static <T> Source<T> fromPayload(Single<T> single) {
    return fromPayloads(Objects.requireNonNull(single).toFlowable());
  }

  static <T> Source<T> from(Maybe<Data<T>> maybe) {
    return new AbstractSource<>(Objects.requireNonNull(maybe).toFlowable(), null, null);
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

  Source<T> orElse(Source<T> alt);

  default String name() {
    return null;
  }

  Optional<T> attr(String key);

  <X> Source<X> map(Function<Data<T>, Data<X>> mapper);

  <X> Source<X> mapItem(Function<T, X> mapper);

  Source<T> filter(Predicate<Data<T>> filter);

  Source<T> filterPayload(Predicate<T> filter);

  Source<T> filterNot(Predicate<Data<T>> filter);

  Source<T> filterPayloadNot(Predicate<T> filter);

  <X> Source<X> flatMap(Function<Data<T>, Publisher<Data<X>>> mapper);

  <X> Source<X> concatMap(Function<Data<T>, Publisher<Data<X>>> mapper);

  <X> Source<X> flatMap(Function<Data<T>, Publisher<Data<X>>> mapper, int maxConcurrency);

  <X> Source<X> flatMapItem(Function<T, Publisher<X>> mapper);

  <X> Source<X> concatMapItem(Function<T, Publisher<X>> mapper);

  <X> Source<X> flatMapItem(Function<T, Publisher<X>> mapper, int maxConcurrency);

  <X> Source<X> reduce(Data<X> zero, BiFunction<Data<X>, Data<T>, Data<X>> function);

  <X> Source<X> reduceItems(X zero, BiFunction<X, T, X> function);

  <X> Source<X> scan(Data<X> zero, BiFunction<Data<X>, Data<T>, Data<X>> function);

  <X> Source<X> scanItems(X zero, BiFunction<X, T, X> function);

  <K> Publisher<GroupedDataStream<K, T>> groupBy(Function<Data<T>, K> keySupplier);

  List<Source<T>> broadcast(int numberOfBranches);

  List<Source<T>> broadcast(String... names);

  Pair<Source<T>, Source<T>> branch(Predicate<Data<T>> condition);

  Pair<Source<T>, Source<T>> branchOnPayload(Predicate<T> condition);

  Sink<T> to(Sink<T> sink);

  Flowable<Data<T>> asFlowable();

  <O> Source<Pair<T, O>> zipWith(Publisher<Data<O>> source);

  Source<Tuple> zipWith(Publisher<Data>... sources);

  Source<Tuple> zipWith(Source... sources);

  Source<T> mergeWith(Publisher<Data<T>> source);

  Source<T> mergeWith(Publisher<Data<T>>... sources);

  <X> Source<X> compose(Function<Publisher<Data<T>>, Publisher<Data<X>>> mapper);

  <X> Source<X> composeFlowable(Function<Flowable<Data<T>>, Flowable<Data<X>>> mapper);

  <X> Source<X> composeItemFlowable(Function<Flowable<T>, Flowable<X>> mapper);
}
