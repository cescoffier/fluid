package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import io.reactivex.flowables.ConnectableFlowable;
import me.escoffier.fluid.constructs.*;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class DataStreamImpl<I, T> implements DataStream<T> {
  private static final String NULL_STREAM_MESSAGE = "The given stream cannot be `null`";
  private static final String NULL_STREAMS_MESSAGE = "The given streams cannot be `null`";
  private Flowable<T> flow;
  private final boolean connectable;
  private final DataStream<I> previous;
  private StreamConnector<T> connector;

  public DataStreamImpl(DataStream<I> previous, Publisher<T> flow) {
    Objects.requireNonNull(flow, "The flow passed to the stream cannot be `null`");
    this.flow = Flowable.fromPublisher(flow);
    this.previous = previous;
    this.connectable = false;
  }

  public DataStreamImpl() {
    connector = new StreamConnector<>();
    this.flow = Flowable.fromPublisher(connector);
    this.previous = null;
    this.connectable = true;
  }


  @Override
  @SafeVarargs
  public final DataStream<T> mergeWith(DataStream<T>... streams) {
    Objects.requireNonNull(streams, NULL_STREAMS_MESSAGE);
    List<DataStream<T>> list = new ArrayList<>();
    list.add(this);
    list.addAll(Arrays.asList(streams));

    Flowable<T> merged = Flowable.merge(list.stream()
      .map(DataStream::flow)
      .collect(Collectors.toList())
    );
    return new DataStreamImpl<>(this, merged);
  }

  @Override
  @SafeVarargs
  public final DataStream<T> concatWith(DataStream<T>... streams) {
    Objects.requireNonNull(streams, NULL_STREAMS_MESSAGE);
    List<DataStream<T>> list = new ArrayList<>();
    list.add(this);
    list.addAll(Arrays.asList(streams));

    Flowable<T> merged = Flowable.concat(list.stream()
      .map(DataStream::flow)
      .collect(Collectors.toList())
    );
    return new DataStreamImpl<>(this, merged);
  }

  @Override
  public <O> DataStream<Pair<T, O>> zipWith(DataStream<O> stream) {
    Objects.requireNonNull(stream, NULL_STREAM_MESSAGE);
    Flowable<Pair<T, O>> flowable = flow.zipWith(stream.flow(), Pair::pair);
    return new DataStreamImpl<>(this, flowable);
  }

  @Override
  public <O1, O2> DataStream<Tuple> zipWith(DataStream<O1> stream1, DataStream<O2> stream2) {
    Objects.requireNonNull(stream1, NULL_STREAM_MESSAGE);
    Objects.requireNonNull(stream2, NULL_STREAM_MESSAGE);
    Flowable<Tuple> flowable = Flowable.zip(flow, stream1.flow(), stream2.flow(), (a, b, c)
      -> Tuple.tuple(a, b, c));
    return new DataStreamImpl<>(this, flowable);
  }

  // TODO Zip up to 7 streams.


  @Override
  public <OUT> DataStream<OUT> transformWith(Transformer<T, OUT> transformer) {
    Objects.requireNonNull(transformer, "The given transformer must not be `null`");
    return new DataStreamImpl<>(this, transformer.transform(flow));
  }

  @Override
  public <OUT> DataStream<OUT> transform(Function<T, OUT> mapper) {
    Objects.requireNonNull(mapper, "The mapper mapper cannot be `null`");
    return new DataStreamImpl<>(this, flow.map(mapper::apply));
  }

  @Override
  public <OUT> DataStream<OUT> transformFlow(Function<Flowable<T>, Flowable<OUT>> mapper) {
    Objects.requireNonNull(mapper, "The mapper mapper cannot be `null'");
    return new DataStreamImpl<>(this, flow.compose(mapper::apply));
  }

  @Override
  public DataStream<T> broadcastTo(DataStream... streams) {
    ConnectableFlowable<T> publish = flow.replay();
    DataStreamImpl<T, T> stream = new DataStreamImpl<>(this, publish);

    for (DataStream s : streams) {
      DataStream first = s;
      while (first.previous() != null) {
        first = first.previous();
      }
      if (first.isConnectable()) {
        first.connect(stream);
      } else {
        throw new IllegalArgumentException("The stream head is not connectable");
      }
    }
    publish.connect();
    return stream;
  }

  public void connect(DataStream<T> source) {
    this.connector.connectDownstream(source);
  }

  @Override
  public DataStream<T> onData(Consumer<? super T> consumer) {
    return new DataStreamImpl<>(this, flow.doOnNext(consumer::accept));
  }

  public DataStream<I> previous() {
    return previous;
  }

  public boolean isConnectable() {
    return connectable;
  }

  @Override
  public void broadcastTo(Sink<T>... sinks) {
    ConnectableFlowable<T> publish = flow.publish();
    for (Sink<T> s : sinks) {
      publish
        .flatMapCompletable(s::dispatch)
        .subscribe();
    }
    publish.connect();
  }

  @Override
  public Flowable<T> flow() {
    return flow;
  }

  // TODO What should be the return type of the "to" operation
  // Sink can produce one result, so maybe a Future. Not a single as it would delay the subscription.

  @Override
  public Sink<T> to(Sink<T> sink) {
    // TODO Error management
    flow
      .flatMapCompletable(sink::dispatch)
      .subscribe(
        () -> {

        },
        Throwable::printStackTrace // TODO Error management
      );

    return sink;
  }
}
