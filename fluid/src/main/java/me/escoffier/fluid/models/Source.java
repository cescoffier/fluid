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

  /**
   * Creates a new {@link Source} from the current one with a name set to the given value.
   *
   * @param name the name, must not be {@code null} or empty
   * @return the new named source
   */
  Source<T> named(String name);

  /**
   * Creates a new {@link Source} from the current one removing the name of the source.
   *
   * @return the new source, without a name.
   */
  Source<T> unnamed();

  /**
   * Creates a new {@link Source} from the current one with a new attribute.
   *
   * @param key   the key, must not be {@code null}
   * @param value the value, must not be {@code null}
   * @return the new source
   */
  Source<T> withAttribute(String key, Object value);

  /**
   * Creates a new {@link Source} from the current one without the attribute with the given name.
   *
   * @param key the name of the attribute to remove, must not be {@code null}. The current source may not contain an
   *            attribute with the given name. In this case, the same set of attribute is given to the new source.
   * @return the new source
   */
  Source<T> withoutAttribute(String key);

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

  static <T> Source<T> from(Message<T>... messages) {
    return from(Flowable.fromArray(Objects.requireNonNull(messages)));
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

  /**
   * Creates a new {@link Source} instance from the current one. This new source switches the source of data to the given
   * one if the current one is empty.
   *
   * @param alt the alternative source used if the current source is empty. Must not be {@code null}, but can be empty.
   * @return the new source
   */
  Source<T> orElse(Source<T> alt);

  /**
   * Default implementation of the {@link #name()} method.
   *
   * @return {@code null}
   */
  default String name() {
    return null;
  }

  /**
   * Retrieves the attribute with the given named. This method returns an {@link Optional} indicating whether or not the
   * source contains the an attribute with the given name.
   *
   * @param key the key, must not be {@code null}
   * @return an {@link Optional} encapsulating the value of the attribute if present, empty otherwise.
   */
  Optional<T> attr(String key);

  /**
   * Creates a new {@link Source} transforms each incoming message from the current source using the given mapper function.
   *
   * @param mapper the mapper function, must not be {@code null}
   * @param <X>    the type of the payload of the resulting messages
   * @return the new source.
   */
  <X> Source<X> map(Function<Message<T>, Message<X>> mapper);

  /**
   * Transforms each incoming payload using the given mapper function.
   *
   * @param mapper the mapper function, must not be {@code null}
   * @param <X>    the type of the payload of the resulting messages
   * @return the new source.
   */
  <X> Source<X> mapPayload(Function<T, X> mapper);

  /**
   * Creates a new {@link Source} discarding all messages not passing the given predicate.
   *
   * @param filter the predicate, must not be {@code null}
   * @return the new source
   */
  Source<T> filter(Predicate<Message<T>> filter);

  /**
   * Creates a new {@link Source} discarding all messages containing payload not passing the given predicate.
   *
   * @param filter the predicate, must not be {@code null}
   * @return the new source
   */
  Source<T> filterPayload(Predicate<T> filter);

  /**
   * Creates a new {@link Source} discarding all messages passing the given predicate.
   *
   * @param filter the predicate, must not be {@code null}
   * @return the new source
   */
  Source<T> filterNot(Predicate<Message<T>> filter);

  /**
   * Creates a new {@link Source} discarding all messages containing payload passing the given predicate.
   *
   * @param filter the predicate, must not be {@code null}
   * @return the new source
   */
  Source<T> filterNotPayload(Predicate<T> filter);

  /**
   * Creates a new {@link Source} taking all messages from the current source and transforming them with the given
   * function. This function can executes asynchronous actions or transforms a single message into a set of messages.
   * The function returns a Publisher, the items are then merged in the returned source.
   *
   * @param mapper the mapper, must not be {@code null}
   * @param <X>    the type of payload contained in the returned messages
   * @return the new source
   */
  <X> Source<X> flatMap(Function<Message<T>, Publisher<Message<X>>> mapper);

  /**
   * Returns a new {@link Source} that emits items resulting from applying the given function each message emitted by the
   * current source. The function returns a Publisher. The new source emits the the items that result from
   * concatenating those resulting Publishers (returned by the function).
   *
   * @param mapper the function, must not be {@code null}
   * @param <X>    the type of payload contained in the returned messages
   * @return the new source
   */
  <X> Source<X> concatMap(Function<Message<T>, Publisher<Message<X>>> mapper);

  /**
   * Creates a new {@link Source} taking all messages from the current source and transforming them with the given
   * function. This function can executes asynchronous actions or transforms a single message into a set of messages.
   * The function returns a Publisher, the items are then merged in the returned source. This method limits the number of
   * publisher subscribed concurrently.
   *
   * @param mapper         the mapper, must not be {@code null}
   * @param maxConcurrency the max number of subscribed publisher for the merge
   * @param <X>            the type of payload contained in the returned messages
   * @return the new source
   */
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
