package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Represents a Stream of Data. On this stream transit instances of {@link Data<T>}.
 *
 * @param <T> the type of the payload encapsulated in the {@link Data} transiting in the stream
 */
public interface DataStream<T> {

  /**
   * Fan-In operator merging the current stream with a set of other streams. All the streams must convey the same type of
   * {@link Data}.
   *
   * @param streams the set of streams to merge. None of them can be {@code null}.
   * @return the new {@link DataStream}.
   */
  @SuppressWarnings("unchecked")
  DataStream<T> mergeWith(DataStream<T>... streams);

  /**
   * Fan-In operator merging the current stream with another stream. The stream must convey the same type of {@link Data}.
   *
   * @param stream the stream to merge. None of them can be {@code null}.
   * @return the new {@link DataStream}.
   */
  DataStream<T> mergeWith(DataStream<T> stream);

  /**
   * Fan-In operator concatenating the current stream with a set of other streams. All the streams must convey the same type of
   * {@link Data}.
   *
   * @param streams the set of streams to merge. None of them can be {@code null}.
   * @return the new {@link DataStream}.
   */
  @SuppressWarnings("unchecked")
  DataStream<T> concatWith(DataStream<T>... streams);

  /**
   * Fan-In operator concatenating the current stream with another stream. The stream must convey the same type of
   * {@link Data}.
   *
   * @param stream the stream to merge. None of them can be {@code null}.
   * @return the new {@link DataStream}.
   */
  DataStream<T> concatWith(DataStream<T> stream);

  /**
   * Fan-in operator associating each {@link Data} transiting on the current stream with a {@link Data} transiting on the
   * given stream. The resulting stream conveys {@link Pair}.
   *
   * @param stream the stream associated with the current one, must not be {@code null}
   * @param <O>    the type of {@link Data} transiting in the <code>stream</code> data stream.
   * @return the new {@link DataStream}.
   */
  <O> DataStream<Pair<T, O>> zipWith(DataStream<O> stream);

  /**
   * Fan-in operator associating each {@link Data} transiting on the current stream with a {@link Data} transiting on the two
   * given streams. The resulting stream conveys {@link Tuple}.
   *
   * @param streams the first stream associated with the current one, must not be {@code null}
   * @return the new {@link DataStream}.
   */
  DataStream<Tuple> zipWith(DataStream... streams);

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
   * Operator transforming each incoming payload into another payload. This method acts on the payloads encapsulated
   * in the transiting {@link Data}.
   * <p>
   * This method is synchronous, for asynchronous processing, use {@link #transformPayloadFlow(Function)}.
   *
   * @param function the function transforming the incoming payload into another payload
   * @param <OUT>    the type of the returned payload
   * @return the new {@link DataStream}.
   */
  <OUT> DataStream<OUT> transformPayload(Function<T, OUT> function);

  /**
   * Operator transforming the flow of incoming payloads. This method acts on the payloads encapsulated in a
   * {@link Data}.
   *
   * @param function the function transforming the incoming flow of payload into another flow of payload
   * @param <OUT>    the type of payload contained into the resulting flow
   * @return the new {@link DataStream}.
   */
  <OUT> DataStream<OUT> transformPayloadFlow(Function<Flowable<T>, Flowable<OUT>> function);

  /**
   * Operator transforming the flow of incoming {@link Data}. This method acts on the {@link Data}, unlike
   * {@link #transformPayloadFlow(Function)} processing the payloads (the encapsulated payloads).
   *
   * @param function the function transforming the incoming flow into another flow
   * @param <OUT>    the type of the data contained into the resulting flow
   * @return the new {@link DataStream}.
   */
  <OUT> DataStream<OUT> transformFlow(Function<Flowable<Data<T>>, Flowable<Data<OUT>>> function);

  /**
   * Fan-out operator dispatching the incoming {@link Data} to a set of streams.
   *
   * @param numberOfBranches the number of branch that will get the incoming data.
   * @return the resulting streams
   */
  List<DataStream<T>> broadcast(int numberOfBranches);

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
  <I> Collection<DataStream<I>> upstreams();

  /**
   * Checks whether or not the current stream can be connected.
   *
   * @return {@code true} if the stream can be connected, {@code false} otherwise
   */
  boolean isConnectable();

  /**
   * Splits the incoming stream in two branches. Data is routed towards one of the branch or the other based on the
   * given predicate. If the predicate returns {@code true} for the incoming data, it is roted to the first branch
   * (left). Otherwise it is routed to the second (right) branch.
   * <p>
   * This construct can be seen as an 'if-then-else' structure:
   * <p>
   * <code>
   * <pre>
   *         if (test(data)) {
   *             => First branch (left)
   *         } else {
   *             => Second branch (right)
   *         }
   *     </pre>
   * </code>
   *
   * @param condition the condition, must not be {@code null}
   * @return the pair of stream representing the two branches.
   */
  Pair<DataStream<T>, DataStream<T>> branch(Predicate<Data<T>> condition);

  /**
   * Same as {@link #branch(Predicate)} but acts on the payload of the data.
   *
   * @param condition the condition, must not be {@code null}
   * @return the pair of stream representing the two branches.
   */
  Pair<DataStream<T>, DataStream<T>> branchOnPayload(Predicate<T> condition);

  /**
   * Splits the incoming stream in several branches (as many as the number of conditions). Data is routed towards on
   * one of the branch based on the first condition returning {@code true} for the incoming data. It can be seen as a
   * 'switch (without default case)'
   * <p>
   * <code>
   * <pre>
   *         switch(data) {
   *             case c1: first branch (index 0); break;
   *             case c2: second branch (index 0); break;
   *             ...
   *         }
   *     </pre>
   * </code>
   * <p>
   * Unlike {@link #branch(Predicate)}, data not matching any conditions are discarded.
   *
   * @param conditions the conditions, must not be {@code null}, must not be empty, none of the condition must be @{code null}
   * @return the list of branches.
   */
  List<DataStream<T>> branch(Predicate<Data<T>>... conditions);

  /**
   * Same as {@link #branch(Predicate[])} but acts on the payload of the data.
   *
   * @param conditions the conditions, must not be {@code null}, must not be empty, none of the condition must be @{code null}
   * @return the list of branches.
   */
  List<DataStream<T>> branchOnPayload(Predicate<T>... conditions);


  /**
   * Connects the current stream to the given <em>source</em>. The passed {@link DataStream} is not necessarily a
   * source, but is the stream that will push data to the current stream once connected.
   *
   * @param source the data stream, must not be {@code null}
   */
  void connect(DataStream<T> source);

  /**
   * NOOP operator invoked on every incoming payload. Useful for tracing, debugging....
   *
   * @param consumer the consumer, must not be {@code null}, should be side-effect free
   * @return the new {@link DataStream}
   */
  DataStream<T> onPayload(Consumer<? super T> consumer);

  /**
   * NOOP operator invoked on every incoming data. Useful for tracing, debugging....
   *
   * @param consumer the consumer, must not be {@code null}, should be side-effect free
   * @return the new {@link DataStream}
   */
  DataStream<T> onData(Consumer<? super Data<T>> consumer);

}
