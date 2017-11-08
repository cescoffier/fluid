package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents a Stream
 */
public interface DataStream<T> {


    DataStream<T> catchAndReturn(Function<Throwable, T> errorHandler);

    DataStream<T> mergeWith(DataStream<T>... streams);

    DataStream<T> concatWith(DataStream<T>... streams);

    <O> DataStream<Pair<T, O>> zipWith(DataStream<O> stream);

    <O1, O2> DataStream<Tuple> zipWith(DataStream<O1> stream1, DataStream<O2> stream2);

    <OUT> DataStream<OUT> transformWith(Transformer<T, OUT> transformer);

    <OUT> DataStream<OUT> transform(Function<T, OUT> function);

    <OUT> DataStream<OUT> transformFlow(Function<Flowable<T>, Flowable<OUT>> function);


    DataStream<T> broadcastTo(@NotNull DataStream... streams);


    void broadcastTo(@NotNull Sink... sinks);

    Flowable<T> flow();

    Sink<T> to(Sink<T> sink);

    static <T> DataStream<T> of(Class<T> clazz) {
        return new DataStreamImpl<>(clazz);
    }

    <I> DataStream<I> previous();

    boolean isConnectable();

    void connect(DataStream<T> source);

    DataStream<T> onData(Consumer<? super T> consumer);
}
