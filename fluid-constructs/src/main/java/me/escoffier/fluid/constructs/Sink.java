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


    static <T> Sink<T> forEach(Consumer<T> consumer) {
        return data -> {
            consumer.accept(data);
            return Completable.complete();
        };
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

    static <T> Sink<T> forEachAsync(Function<T, Completable> fun) {
        return fun::apply;
    }

    static <OUT, RES> ScanSink<OUT, RES> fold(RES init, BiFunction<OUT, RES, RES> mapper) {
        return new ScanSink<>(Objects.requireNonNull(init), Objects.requireNonNull(mapper));
    }

    static <T> HeadSink<T> head() {
        return new HeadSink<>();
    }

    class ScanSink<OUT, RES> implements Sink<OUT> {
        private final BiFunction<OUT, RES, RES> mapper;
        private RES current;

        // TODO provide a flowable to collect the "current" values.

        public ScanSink(RES init, BiFunction<OUT, RES, RES> mapper) {
            this.mapper = mapper;
            this.current = init;
        }

        @Override
        public synchronized Completable dispatch(OUT data) {
            current = mapper.apply(data, current);
            return Completable.complete();
        }

        RES value() {
            return current;
        }
    }

    class HeadSink<OUT> implements Sink<OUT> {
        private volatile OUT head;

        @Override
        public Completable dispatch(OUT data) {
            synchronized (this) {
                if (head == null) {
                    // TODO It would be nice to be able to cancel the subscription
                    head = data;
                }
            }
            return Completable.complete();
        }

        synchronized OUT value() {
            return head;
        }
    }

}
