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
 * Represents a data source. It emits {@link Message <T>}.
 */
public interface Source<T> extends Publisher<Message<T>> {

  Source<T> named(String name);

  Source<T> withAttribute(String key, Object value);

  static <T> Source<T> from(Publisher<Message<T>> flow) {
    return from(Flowable.fromPublisher(Objects.requireNonNull(flow)));
  }

  static <T> Source<T> fromPayloads(Publisher<T> flow) {
    return fromPayloads(Flowable.fromPublisher(Objects.requireNonNull(flow)));
  }

  static <T> Source<T> from(Flowable<Message<T>> flow) {
    return new DefaultSource<>(Objects.requireNonNull(flow), null, null);
  }

  static <T> Source<T> fromPayloads(Flowable<T> flow) {
    return new DefaultSource<>(Objects.requireNonNull(flow).map(Message::new), null, null);
  }

  static <T> Source<T> from(Single<Message<T>> single) {
    return from(Objects.requireNonNull(single).toFlowable());
  }

  static <T> Source<T> fromPayload(Single<T> single) {
    return fromPayloads(Objects.requireNonNull(single).toFlowable());
  }

  static <T> Source<T> from(Maybe<Message<T>> maybe) {
    return new DefaultSource<>(Objects.requireNonNull(maybe).toFlowable(), null, null);
  }

  static <T> Source<T> fromPayload(Maybe<T> maybe) {
    return fromPayloads(Objects.requireNonNull(maybe).toFlowable());
  }

  static <T> Source<T> from(Message<T>... payloads) {
    return from(Flowable.fromArray(Objects.requireNonNull(payloads)));
  }

  static <T> Source<T> from(T... payloads) {
    return fromPayloads(Flowable.fromArray(Objects.requireNonNull(payloads)));
  }

  static <T> Source<T> from(Iterable<Message<T>> payloads) {
    return from(Flowable.fromIterable(Objects.requireNonNull(payloads)));
  }

  static <T> Source<T> fromPayloads(Iterable<T> payloads) {
    return fromPayloads(Flowable.fromIterable(Objects.requireNonNull(payloads)));
  }

  static <T> Source<T> from(java.util.stream.Stream<Message<T>> stream) {
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

  static <T> Source<T> just(Message<T> payload) {
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

  <X> Source<X> map(Function<Message<T>, Message<X>> mapper);

  <X> Source<X> mapPayload(Function<T, X> mapper);

  Source<T> filter(Predicate<Message<T>> filter);

  Source<T> filterPayload(Predicate<T> filter);

  Source<T> filterNot(Predicate<Message<T>> filter);

  Source<T> filterPayloadNot(Predicate<T> filter);

  <X> Source<X> flatMap(Function<Message<T>, Publisher<Message<X>>> mapper);

  <X> Source<X> concatMap(Function<Message<T>, Publisher<Message<X>>> mapper);

  <X> Source<X> flatMap(Function<Message<T>, Publisher<Message<X>>> mapper, int maxConcurrency);

  <X> Source<X> flatMapPayload(Function<T, Publisher<X>> mapper);

  <X> Source<X> concatMapPayload(Function<T, Publisher<X>> mapper);

  <X> Source<X> flatMapPayload(Function<T, Publisher<X>> mapper, int maxConcurrency);

  <X> Source<X> reduce(Message<X> zero, BiFunction<Message<X>, Message<T>, Message<X>> function);

  <X> Source<X> reducePayloads(X zero, BiFunction<X, T, X> function);

  <X> Source<X> scan(Message<X> zero, BiFunction<Message<X>, Message<T>, Message<X>> function);

  <X> Source<X> scanPayloads(X zero, BiFunction<X, T, X> function);

  <K> Publisher<GroupedDataStream<K, T>> groupBy(Function<Message<T>, K> keySupplier);

  List<Source<T>> broadcast(int numberOfBranches);

  List<Source<T>> broadcast(String... names);

  Pair<Source<T>, Source<T>> branch(Predicate<Message<T>> condition);

  Pair<Source<T>, Source<T>> branchOnPayload(Predicate<T> condition);

  Sink<T> to(Sink<T> sink);

  Flowable<Message<T>> asFlowable();

  <O> Source<Pair<T, O>> zipWith(Publisher<Message<O>> source);

  Source<Tuple> zipWith(Publisher<Message>... sources);

  Source<Tuple> zipWith(Source... sources);

  Source<T> mergeWith(Publisher<Message<T>> source);

  Source<T> mergeWith(Publisher<Message<T>>... sources);

  <X> Source<X> compose(Function<Publisher<Message<T>>, Publisher<Message<X>>> mapper);

  <X> Source<X> composeFlowable(Function<Flowable<Message<T>>, Flowable<Message<X>>> mapper);

  <X> Source<X> composePayloadFlowable(Function<Flowable<T>, Flowable<X>> mapper);
}
