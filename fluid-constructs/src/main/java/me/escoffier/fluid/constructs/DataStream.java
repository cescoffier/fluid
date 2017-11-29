package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents a Stream of Data. On this stream transit instances of {@link Data<T>}.
 *
 * @param <T> the type of the item encapsulated in the {@link Data} transiting in the stream
 */
public interface DataStream<T> {

  /**
   * Operator merging the current stream with a set of other streams. All the streams must convey the same type of
   * {@link Data}.
   *
   * @param streams the set of streams to merge. None of them can be {@code null}.
   * @return the new {@link DataStream}.
   */
  @SuppressWarnings("unchecked")
  DataStream<T> mergeWith(DataStream<T>... streams);

  /**
   * Operator merging the current stream with another stream. The stream must convey the same type of {@link Data}.
   *
   * @param stream the stream to merge. None of them can be {@code null}.
   * @return the new {@link DataStream}.
   */
  DataStream<T> mergeWith(DataStream<T> stream);

  /**
   * Operator concatenating the current stream with a set of other streams. All the streams must convey the same type of
   * {@link Data}.
   *
   * @param streams the set of streams to merge. None of them can be {@code null}.
   * @return the new {@link DataStream}.
   */
  @SuppressWarnings("unchecked")
  DataStream<T> concatWith(DataStream<T>... streams);

  /**
   * Operator concatenating the current stream with another stream. The stream must convey the same type of
   * {@link Data}.
   *
   * @param stream the stream to merge. None of them can be {@code null}.
   * @return the new {@link DataStream}.
   */
  DataStream<T> concatWith(DataStream<T> stream);

  /**
   * Operator associating each {@link Data} transiting on the current stream with a {@link Data} transiting on the
   * given stream. The resulting stream conveys {@link Pair}.
   *
   * @param stream the stream associated with the current one, must not be {@code null}
   * @param <O>    the type of {@link Data} transiting in the <code>stream</code> data stream.
   * @return the new {@link DataStream}.
   */
  <O> DataStream<Pair<T, O>> zipWith(DataStream<O> stream);

  /**
   * Operator associating each {@link Data} transiting on the current stream with a {@link Data} transiting on the two
   * given streams. The resulting stream conveys {@link Tuple}.
   *
   * @param stream1 the first stream associated with the current one, must not be {@code null}
   * @param stream2 the second stream associated with the current one, must not be {@code null}
   * @param <O1>    the type of {@link Data} transiting in the <code>stream1</code> data stream.
   * @param <O2>    the type of {@link Data} transiting in the <code>stream2</code> data stream.
   * @return the new {@link DataStream}.
   */
  <O1, O2> DataStream<Tuple> zipWith(DataStream<O1> stream1, DataStream<O2> stream2);

  /**
   * Operator transforming each incoming {@code Data} into another {@code Data}.
   * <p>
   * This method is synchronous, for asynchronous processing, use {@link #transformFlow(Function)}.
   *
   * @param function the function applied to each incoming {@code Data} and producing another {@link Data}.
   * @param <OUT>    the type of the resulting data.
   * @return the new {@link DataStream}.
   */
  <OUT> DataStream<OUT> transform(Function<Data<T>, Data<OUT>> function);

  /**
   * Operator transforming each incoming item into another item. This method acts on the items encapsulated in a
   * {@link Data}.
   * <p>
   * This method is synchronous, for asynchronous processing, use {@link #transformItemFlow(Function)}.
   *
   * @param function the function transforming the incoming item into another item
   * @param <OUT>    the type of the returned item
   * @return the new {@link DataStream}.
   */
  <OUT> DataStream<OUT> transformItem(Function<T, OUT> function);

  /**
   * Operator transforming the flow of incoming items. This method acts on the items encapsulated in a
   * {@link Data}.
   *
   * @param function the function transforming the incoming flow into another flow
   * @param <OUT>    the type of item contained into the resulting flow
   * @return the new {@link DataStream}.
   */
  <OUT> DataStream<OUT> transformItemFlow(Function<Flowable<T>, Flowable<OUT>> function);

  /**
   * Operator transforming the flow of incoming {@link Data}. This method acts on the {@link Data}, unlike
   * {@link #transformItemFlow(Function)} processing the items (the encapsulated items).
   *
   * @param function the function transforming the incoming flow into another flow
   * @param <OUT>    the type of the data contained into the resulting flow
   * @return the new {@link DataStream}.
   */
  <OUT> DataStream<OUT> transformFlow(Function<Flowable<Data<T>>, Flowable<Data<OUT>>> function);

  /**
   * Fan-out operator dispatching the incoming {@link Data} to a set of streams.
   *
   * @param streams the streams receiving the data, none of them can be {@code null}
   * @return the resulting streams
   */
  DataStream<T> broadcastTo(DataStream... streams);

  /**
   * @return the underlying flow of {@link Data}.
   */
  Flowable<Data<T>> flow();

  /**
   * Operator terminating the transformation. It sends the incoming data to a {@link Sink} that will dispatch the
   * data to the next node, message broker....
   *
   * @param sink the sink, must not be {@code null}
   * @return the sink
   */
  Sink<T> to(Sink<T> sink);

  /**
   * Creates a non-connected data streams. A not connected data stream is not connected to a data source, meaning it
   * is not functional until it is. This method is used when you need to structures your transformation is different
   * chunk and connect them afterward.
   *
   * @param clazz the type of data expected by the stream.
   * @param <T>   the type of data expected by the stream.
   * @return the data stream
   */
  static <T> DataStream<T> of(Class<T> clazz) {
    return new DataStreamImpl<>();
  }

  /**
   * Internal API allowing to retrieves the stream on which the current stream is connected to.
   *
   * @param <I> the type of data received by the previous stream.
   * @return the previous stream, may be {@code null} if the current stream is a {@link Source}.
   */
  <I> DataStream<I> previous();

  /**
   * Checks whether or not the current stream can be connected.
   *
   * @return {@code true} if the stream can be connected, {@code false} otherwise
   */
  boolean isConnectable();

  /**
   * Connects the current stream to the given <em>source</em>. The passed {@link DataStream} is not necessarily a
   * source, but is the stream that will push data to the current stream once connected.
   *
   * @param source the data stream, must not be {@code null}
   */
  void connect(DataStream<T> source);

  /**
   * NOOP operator invoked on every incoming item. Useful for tracing, debugging....
   *
   * @param consumer the consumer, must not be {@code null}, should be side-effect free
   * @return the new {@link DataStream}
   */
  DataStream<T> onItem(Consumer<? super T> consumer);

  /**
   * NOOP operator invoked on every incoming data. Useful for tracing, debugging....
   *
   * @param consumer the consumer, must not be {@code null}, should be side-effect free
   * @return the new {@link DataStream}
   */
  DataStream<T> onData(Consumer<? super Data<T>> consumer);
}
