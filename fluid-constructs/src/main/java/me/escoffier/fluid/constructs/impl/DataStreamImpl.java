package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.*;
import org.reactivestreams.Publisher;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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
  private static final String NULL_FUNCTION_MESSAGE = "The given function must not be `null`";

  private final boolean connectable;
  private final Flowable<Data<T>> flow;
  private StreamConnector<T> connector;

  private Collection<DataStream> downstreams;
  private Collection<DataStream> upstreams = Collections.emptyList();

  private AtomicInteger upstreamReady = new AtomicInteger();

  public DataStreamImpl(Flowable<Data<T>> flowable) {
    this(Collections.emptyList(), flowable);
  }

  public DataStreamImpl(DataStream<T> previous) {
    this();
    this.downstreams = Collections.singletonList(previous);
  }

  public void setNext(DataStream... next) {
    upstreams = Arrays.asList(next);
  }

  public DataStreamImpl(DataStream previous, Publisher<Data<T>> flowable) {
    Objects.requireNonNull(flowable, "The flowable passed to the stream cannot be `null`");
    this.connectable = false;

    if (previous == null) {
      this.downstreams = Collections.emptyList();
    } else {
      this.downstreams = Collections.singletonList(previous);
    }

    this.flow = Flowable.fromPublisher(flowable);

  }

  public DataStreamImpl(Collection<DataStream> previous, Flowable<Data<T>> flowable) {
    Objects.requireNonNull(flowable, "The flowable passed to the stream cannot be `null`");
    this.connectable = false;
    this.downstreams = Collections.unmodifiableList(new ArrayList<>(previous));

    this.flow = flowable;
  }

  public DataStreamImpl() {
    this.connector = new StreamConnector<>();
    this.connectable = true;
    this.downstreams = Collections.emptyList();
    this.flow = Flowable.fromPublisher(connector);
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

    Collection<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.addAll(Arrays.asList(streams));

    Arrays.stream(streams).forEach(d -> ((DataStreamImpl) d).setNext(this));

    return new DataStreamImpl<>(previous, merged);
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
    Collection<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.add(stream);

    ((DataStreamImpl) stream).setNext(this);
    DataStreamImpl<Object, T> next = new DataStreamImpl<>(previous, merged);
    this.setNext(next);
    return next;
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

    Collection<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.addAll(Arrays.asList(streams));

    DataStreamImpl<Object, T> next = new DataStreamImpl<>(previous, merged);
    setNext(next);
    return next;
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

    Collection<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.add(stream);

    DataStreamImpl<Object, T> next = new DataStreamImpl<>(previous, merged);
    setNext(next);
    return next;
  }

  @Override
  public <O> DataStream<Pair<T, O>> zipWith(DataStream<O> stream) {
    Objects.requireNonNull(stream, NULL_STREAM_MESSAGE);
    Flowable<Data<Pair<T, O>>> flowable = flow.zipWith(stream.flow(),
      (d1, d2) -> new Data<>(pair(d1.payload(), d2.payload())));

    Collection<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.add(stream);

    DataStreamImpl<Object, Pair<T, O>> next = new DataStreamImpl<>(previous, flowable);
    setNext(next);
    ((DataStreamImpl) stream).setNext(next);

    return next;
  }

  @Override
  public <O1, O2> DataStream<Tuple> zipWith(DataStream<O1> stream1, DataStream<O2> stream2) {
    Objects.requireNonNull(stream1, NULL_STREAM_MESSAGE);
    Objects.requireNonNull(stream2, NULL_STREAM_MESSAGE);
    Flowable<Data<Tuple>> flowable = Flowable
      .zip(flow, stream1.flow(), stream2.flow(),
        (a, b, c) -> new Data<>(Tuple.tuple(a.payload(), b.payload(), c.payload())));

    Collection<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.add(stream1);
    previous.add(stream2);

    DataStreamImpl<Object, Tuple> next = new DataStreamImpl<>(previous, flowable);
    this.setNext(next);
    return next;
  }

  // TODO Zip up to 7 streams.


  @Override
  public <OUT> DataStream<OUT> transform(Function<Data<T>, Data<OUT>> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);
    DataStreamImpl<Object, OUT> next = new DataStreamImpl<>(this, flow.map(function::apply));
    setNext(next);
    return next;
  }

  @Override
  public <OUT> DataStream<OUT> transformPayload(Function<T, OUT> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);
    // TODO we are loosing the headers.
    DataStreamImpl<Object, OUT> next = new DataStreamImpl<>(this,
      flow.map(Data::payload).map(function::apply).map(Data::new));
    setNext(next);
    return next;
  }

  @Override
  public <OUT> DataStream<OUT> transformPayloadFlow(Function<Flowable<T>, Flowable<OUT>> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);
    // TODO we are loosing the headers.

    Flowable<Data<OUT>> flowable = function.apply(flow.map(Data::payload)).map(Data::new);
    DataStreamImpl<Object, OUT> next = new DataStreamImpl<>(this, flowable);
    setNext(next);
    return next;
  }

  @Override
  public <OUT> DataStream<OUT> transformFlow(Function<Flowable<Data<T>>, Flowable<Data<OUT>>> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);

    DataStreamImpl<Object, OUT> next = new DataStreamImpl<>(this, function.apply(flow));
    setNext(next);
    return next;

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

    stream.setNext(streams.toArray(new DataStream[streams.size()]));
    this.setNext(stream);

    return streams;
  }

  @Override
  public Pair<DataStream<T>, DataStream<T>> branch(Predicate<Data<T>> condition) {
    DataStream<T> success = new DataStreamImpl<T, T>();
    DataStream<T> failure = new DataStreamImpl<T, T>();

    Branch<T> build = new Branch.BranchBuilder<T>().add(condition, success).addFallback(failure).build();
    DataStreamImpl<T, T> stream = new DataStreamImpl<>(this, build);

    success.connect(stream);
    failure.connect(stream);
    build.connect(this);

    stream.setNext(success, failure);
    this.setNext(stream);

    return pair(success, failure);
  }

  @Override
  public Pair<DataStream<T>, DataStream<T>> branchOnPayload(Predicate<T> condition) {
    DataStream<T> success = new DataStreamImpl<T, T>();
    DataStream<T> failure = new DataStreamImpl<T, T>();

    Branch<T> build = new Branch.BranchBuilder<T>().add(x -> condition.test(x.payload()), success)
      .addFallback(failure).build();
    DataStreamImpl<T, T> stream = new DataStreamImpl<>(this, build);

    success.connect(stream);
    failure.connect(stream);
    build.connect(this);

    stream.setNext(success, failure);
    this.setNext(stream);

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
    DataStreamImpl<T, T> stream = new DataStreamImpl<>(this, built);

    for (DataStream<T> b : branches) {
      b.connect(stream);
    }
    built.connect(this);

    stream.setNext(branches.toArray(new DataStream[branches.size()]));
    this.setNext(stream);

    return branches;
  }

  @Override
  public List<DataStream<T>> branchOnPayload(Predicate<T>... conditions) {
    List<DataStream<T>> branches = new ArrayList<>();
    Branch.BranchBuilder<T> builder = new Branch.BranchBuilder<>();
    for (Predicate<T> predicate : conditions) {
      DataStream<T> stream = new DataStreamImpl<T, T>();
      builder.add(x -> predicate.test(x.payload()), stream);
      branches.add(stream);
    }
    Branch<T> built = builder.build();
    DataStreamImpl<T, T> stream = new DataStreamImpl<>(this, built);
    for (DataStream<T> b : branches) {
      b.connect(stream);
    }
    built.connect(this);
    stream.setNext(branches.toArray(new DataStream[branches.size()]));
    this.setNext(stream);

    return branches;
  }

  public void connect(DataStream<T> source) {
    downstreams = Collections.singletonList(source);
    System.out.println("Connector: " + this + ", upstreams: " + upstreams);
    this.connector.connectDownstream(source);
  }

  @Override
  public DataStream<T> onData(Consumer<? super Data<T>> consumer) {
    DataStreamImpl<Object, T> next = new DataStreamImpl<>(this, flow
      .doOnNext(consumer::accept));
    this.setNext(next);
    return next;
  }

  @Override
  public DataStream<T> onPayload(Consumer<? super T> consumer) {
    DataStreamImpl<Object, T> next = new DataStreamImpl<>(this,
      flow.doOnNext(d -> consumer.accept(d.payload())));
    this.setNext(next);
    return next;
  }

  public boolean isConnectable() {
    return connectable;
  }

  @Override
  public Flowable<Data<T>> flow() {
    return flow;
  }

  @Override
  public Collection<DataStream> downtreams() {
    return downstreams;
  }

  @Override
  public Collection<DataStream> upstreams() {
    return upstreams;
  }

  // TODO What should be the return type of the "to" operation
  // Sink can produce one result, so maybe a Future. Not a single as it would delay the subscription.

  public void ready(DataStream upstream) {
    int i = upstreamReady.incrementAndGet();
    if (i >= upstreams.size()) {
      System.out.println(this + " downstreams: " + downstreams);
      downstreams.forEach(d -> d.ready(this));
    }
  }

  @Override
  public Sink<T> to(Sink<T> sink) {
    // Notify ready
    DataStreamImpl last = new DataStreamImpl(
      this,
      flow
        .toList()
        .flatMapCompletable(sink::dispatch)
        .toFlowable()
    );
    this.setNext(last);

    System.out.println("Sink is " + last);

    ready(this);
    return sink;
  }

  public StreamConnector<T> connector() {
    return connector;
  }
}
