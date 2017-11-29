package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents a Stream.
 */
public interface DataStream<T> {

  DataStream<T> mergeWith(DataStream<T>... streams);

  DataStream<T> concatWith(DataStream<T>... streams);

  <O> DataStream<Pair<T, O>> zipWith(DataStream<O> stream);

  <O1, O2> DataStream<Tuple> zipWith(DataStream<O1> stream1, DataStream<O2> stream2);

  <OUT> DataStream<OUT> transform(Transformer<Data<T>, Data<OUT>> transformer);

  <OUT> DataStream<OUT> transform(Function<Data<T>, Data<OUT>> function);

  <OUT> DataStream<OUT> transformItem(Function<T, OUT> function);

  <OUT> DataStream<OUT> transformItemFlow(Function<Flowable<T>, Flowable<OUT>> function);

  <OUT> DataStream<OUT> transformFlow(Function<Flowable<Data<T>>,
    Flowable<Data<OUT>>> function);


  DataStream<T> broadcastTo(DataStream... streams);

  Flowable<Data<T>> flow();

  Sink<T> to(Sink<T> sink);

  static <T> DataStream<T> of(Class<T> clazz) {
    return new DataStreamImpl<>();
  }

  <I> DataStream<I> previous();

  boolean isConnectable();

  void connect(DataStream<T> source);

  DataStream<T> onItem(Consumer<? super T> consumer);

  DataStream<T> onData(Consumer<? super Data<T>> consumer);
}
