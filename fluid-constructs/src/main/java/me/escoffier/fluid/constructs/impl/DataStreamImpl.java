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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static me.escoffier.fluid.constructs.Pair.pair;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class DataStreamImpl<I, T> implements DataStream<T> {
  private static final String NULL_STREAM_MESSAGE = "The given stream cannot be `null`";
  private static final String NULL_STREAMS_MESSAGE = "The given streams cannot be `null`";
  protected Flowable<Data<T>> flow;
  private final boolean connectable;
  private final DataStream<I> previous;
  private StreamConnector<T> connector;

  public DataStreamImpl(DataStream<I> previous, Publisher<Data<T>> flow) {
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

    Flowable<Data<T>> merged = Flowable.merge(list.stream()
      .map(DataStream::flow)
      .collect(Collectors.toList())
    );
    return new DataStreamImpl<>(this, merged);
  }

  @Override
  public final DataStream<T> mergeWith(DataStream<T> stream) {
    Objects.requireNonNull(stream, NULL_STREAMS_MESSAGE);
    List<DataStream<T>> list = new ArrayList<>();
    list.add(this);
    list.add(stream);

    Flowable<Data<T>> merged = Flowable.merge(list.stream()
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

    Flowable<Data<T>> merged = Flowable.concat(list.stream()
      .map(DataStream::flow)
      .collect(Collectors.toList())
    );
    return new DataStreamImpl<>(this, merged);
  }

  @Override
  public final DataStream<T> concatWith(DataStream<T> stream) {
    Objects.requireNonNull(stream, NULL_STREAMS_MESSAGE);
    List<DataStream<T>> list = new ArrayList<>();
    list.add(this);
    list.add(stream);
    Flowable<Data<T>> merged = Flowable.concat(list.stream()
      .map(DataStream::flow)
      .collect(Collectors.toList())
    );
    return new DataStreamImpl<>(this, merged);
  }

  @Override
  public <O> DataStream<Pair<T, O>> zipWith(DataStream<O> stream) {
    Objects.requireNonNull(stream, NULL_STREAM_MESSAGE);
    Flowable<Data<Pair<T, O>>> flowable = flow.zipWith(stream.flow(),
      (d1, d2) -> new Data<>(pair(d1.item(), d2.item())));
    return new DataStreamImpl<>(this, flowable);
  }

  @Override
  public <O1, O2> DataStream<Tuple> zipWith(DataStream<O1> stream1, DataStream<O2> stream2) {
    Objects.requireNonNull(stream1, NULL_STREAM_MESSAGE);
    Objects.requireNonNull(stream2, NULL_STREAM_MESSAGE);
    Flowable<Data<Tuple>> flowable = Flowable
      .zip(flow, stream1.flow(), stream2.flow(),
        (a, b, c) -> new Data<>(Tuple.tuple(a.item(), b.item(), c.item())));
    return new DataStreamImpl<>(this, flowable);
  }

  // TODO Zip up to 7 streams.


  @Override
  public <OUT> DataStream<OUT> transform(Function<Data<T>, Data<OUT>> function) {
    Objects.requireNonNull(function, "The given function must not be `null`");
    return new DataStreamImpl<>(this, flow.map(function::apply));
  }

  @Override
  public <OUT> DataStream<OUT> transformItem(Function<T, OUT> function) {
    Objects.requireNonNull(function, "The given function must not be `null`");
    // TODO we are loosing the headers.
    return new DataStreamImpl<>(this,
      flow.map(Data::item).map(function::apply).map(Data::new));
  }

  @Override
  public <OUT> DataStream<OUT> transformItemFlow(Function<Flowable<T>, Flowable<OUT>> function) {
    Objects.requireNonNull(function, "The given function must not be `null`");
    // TODO we are loosing the headers.

    Flowable<Data<OUT>> flowable = function.apply(flow.map(Data::item)).map(Data::new);
    return new DataStreamImpl<>(this, flowable);
  }

  @Override
  public <OUT> DataStream<OUT> transformFlow(Function<Flowable<Data<T>>, Flowable<Data<OUT>>> function) {
    Objects.requireNonNull(function, "The given function must not be `null`");

    return new DataStreamImpl<>(this, function.apply(flow));

  }

  @Override
  public List<DataStream<T>> broadcast(int numberOfBranches) {
    if (numberOfBranches <= 1) {
      throw new IllegalArgumentException("The number of branch must be at least 2");
    }

    List<DataStream<T>> streams = new ArrayList<>(numberOfBranches);
    Flowable<Data<T>> publish = flow.publish().autoConnect(numberOfBranches);
    DataStreamImpl<T, T> stream = new DataStreamImpl<>(this, publish);

    for (int i = 0; i < numberOfBranches; i++) {
      DataStream<T> branch = new DataStreamImpl<>();
      streams.add(branch);
      branch.connect(stream);
    }

    return streams;
  }


//  @SafeVarargs
//  public final DataStream<T> broadcastTo(DataStream... streams) {
//    ConnectableFlowable<Data<T>> publish = flow.replay();
//
//    for (DataStream s : streams) {
//      DataStream first = s;
//      while (first.previous() != null) {
//        first = first.previous();
//      }
//      if (first.isConnectable()) {
//        first.connect(stream);
//      } else {
//        throw new IllegalArgumentException("The stream head is not connectable");
//      }
//    }
//    publish.connect();
//    return stream;
//  }

  @Override
  public Pair<DataStream<T>, DataStream<T>> branch(Predicate<Data<T>> condition) {
    DataStream<T> success = new DataStreamImpl<T, T>();
    DataStream<T> failure = new DataStreamImpl<T, T>();

    Branch<T> build = new Branch.BranchBuilder<T>().add(condition, success).addFallback(failure).build();
    DataStream<T> stream = new DataStreamImpl<>(this, build);

    success.connect(stream);
    failure.connect(stream);
    build.connect(this);

    return pair(success, failure);
  }

  @Override
  public Pair<DataStream<T>, DataStream<T>> branchOnItem(Predicate<T> condition) {
    DataStream<T> success = new DataStreamImpl<T, T>();
    DataStream<T> failure = new DataStreamImpl<T, T>();

    Branch<T> build = new Branch.BranchBuilder<T>().add(x -> condition.test(x.item()), success)
      .addFallback(failure).build();
    DataStream<T> stream = new DataStreamImpl<>(this, build);

    success.connect(stream);
    failure.connect(stream);
    build.connect(this);

    return pair(success, failure);
  }

  @Override
  public List<DataStream<T>> branch(Predicate<Data<T>>... conditions) {
    List<DataStream<T>> branches = new ArrayList<>();
    Branch.BranchBuilder<T> builder = new Branch.BranchBuilder<>();
    for (Predicate<Data<T>> predicate : conditions) {
      DataStream<T> stream = new DataStreamImpl<T, T>();
      builder.add(predicate, stream);
      branches.add(stream);
    }
    Branch<T> built = builder.build();
    DataStream<T> stream = new DataStreamImpl<>(this, built);
    for (DataStream<T> b : branches) {
      b.connect(stream);
    }
    built.connect(this);
    return branches;
  }

  @Override
  public List<DataStream<T>> branchOnItem(Predicate<T>... conditions) {
    List<DataStream<T>> branches = new ArrayList<>();
    Branch.BranchBuilder<T> builder = new Branch.BranchBuilder<>();
    for (Predicate<T> predicate : conditions) {
      DataStream<T> stream = new DataStreamImpl<T, T>();
      builder.add(x -> predicate.test(x.item()), stream);
      branches.add(stream);
    }
    Branch<T> built = builder.build();
    DataStream<T> stream = new DataStreamImpl<>(this, built);
    for (DataStream<T> b : branches) {
      b.connect(stream);
    }
    built.connect(this);
    return branches;
  }

  public void connect(DataStream<T> source) {
    this.connector.connectDownstream(source);
  }

  @Override
  public DataStream<T> onData(Consumer<? super Data<T>> consumer) {
    return new DataStreamImpl<>(this, flow.doOnNext(consumer::accept));
  }

  @Override
  public DataStream<T> onItem(Consumer<? super T> consumer) {
    return new DataStreamImpl<>(this,
      flow.doOnNext(d -> consumer.accept(d.item())));
  }

  public DataStream<I> previous() {
    return previous;
  }

  public boolean isConnectable() {
    return connectable;
  }

  @Override
  public Flowable<Data<T>> flow() {
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

  StreamConnector<T> connector() {
    return connector;
  }
}
