package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;
import me.escoffier.fluid.constructs.impl.SourceImpl;
import org.reactivestreams.Publisher;

import java.util.Objects;

/**
 * Represents a data source.
 */
public interface Source<T> extends DataStream<T> {

    default Source<T> orElse(Source<T> alt) {
        return new SourceImpl<>(this.flow().switchIfEmpty(alt.flow()));
    }

    static <T> Source<T> from(Publisher<T> flow) {
        return new SourceImpl<>(flow);
    }

    static <T> Source<T> from(Flowable<T> flow) {
        return new SourceImpl<>(flow);
    }

    static <T> Source<T> from(Single<T> single) {
        return new SourceImpl<>(single.toFlowable());
    }

    static <T> Source<T> from(Maybe<T> single) {
        return new SourceImpl<>(single.toFlowable());
    }

    static <T> Source<T> from(T... items) {
        return new SourceImpl<>(Flowable.fromArray(items));
    }

    static <T> Source<T> from(Iterable<T> items) {
        return new SourceImpl<>(Flowable.fromIterable(items));
    }

    static <T> Source<T> from(java.util.stream.Stream<T> stream) {
        return new SourceImpl<>(Flowable.fromIterable(stream::iterator));
    }

    static <T> Source<T> empty() {
        return new SourceImpl<>(Flowable.empty());
    }

    static <T> Source<T> just(T item) {
        return new SourceImpl<>(Flowable.just(item));
    }

    static <T> Source<T> failed() {
        return new SourceImpl<>(Flowable.error(new Exception("Source failure")));
    }

    static <T> Source<T> failed(Throwable t) {
        return new SourceImpl<>(Flowable.error(Objects.requireNonNull(t)));
    }

    default String name() {
        return null;
    }


}
