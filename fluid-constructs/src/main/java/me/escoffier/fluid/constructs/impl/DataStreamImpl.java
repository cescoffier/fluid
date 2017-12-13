package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.*;
import org.reactivestreams.Publisher;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static me.escoffier.fluid.constructs.Pair.pair;

/**
 * Implementation of Data Stream.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class DataStreamImpl<I, T> implements DataStream<T> {
  private static final String NULL_STREAM_MESSAGE = "The given stream cannot be `null`";
  private static final String NULL_STREAMS_MESSAGE = "The given streams cannot be `null`";
  private static final String NULL_FUNCTION_MESSAGE = "The given function must not be `null`";

  protected Flowable<Data<T>> flow;
  private final boolean connectable;
  private Collection<DataStream> upstreams;
  private StreamConnector<T> connector;

  public DataStreamImpl(DataStream<I> upstreams, Publisher<Data<T>> flow) {
    this(Collections.singletonList(upstreams), flow);
  }

  public DataStreamImpl(List<DataStream> upstreams, Publisher<Data<T>> flow) {
    Objects.requireNonNull(flow, "The flow passed to the stream cannot be `null`");
    this.flow = Flowable.fromPublisher(flow);
    this.upstreams = Collections.unmodifiableList(upstreams);
    this.connectable = false;
  }

  public DataStreamImpl() {
    this(Collections.emptyList());
  }

  public DataStreamImpl(List<DataStream<I>> upstreams) {
    connector = new StreamConnector<>();
    this.flow = Flowable.fromPublisher(connector);
    this.upstreams = Collections.unmodifiableList(upstreams);
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

    List<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.addAll(Arrays.asList(streams));

    return new DataStreamImpl<>(previous, merged);
  }

  @SuppressWarnings("unchecked")
  @Override
  public final DataStream<T> mergeWith(DataStream<T> stream) {
    DataStream[] array = {stream};
    return mergeWith(array);
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

    List<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.addAll(Arrays.asList(streams));

    return new DataStreamImpl<>(previous, merged);
  }

  @SuppressWarnings("unchecked")
  @Override
  public final DataStream<T> concatWith(DataStream<T> stream) {
    DataStream[] array = {stream};
    return concatWith(array);
  }

  @Override
  public <O> DataStream<Pair<T, O>> zipWith(DataStream<O> stream) {
    Objects.requireNonNull(stream, NULL_STREAM_MESSAGE);
    Flowable<Data<Pair<T, O>>> flowable = flow.zipWith(stream.flow(),
      (d1, d2) -> new Data<>(pair(d1.payload(), d2.payload())));

    List<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.add(stream);

    return new DataStreamImpl<>(previous, flowable);
  }

  @Override
  public DataStream<Tuple> zipWith(DataStream... streams) {
    Objects.requireNonNull(streams, NULL_STREAM_MESSAGE);

    List<Flowable<?>> toBeZipped = new ArrayList<>();
    List<DataStream> previous = new ArrayList<>();
    toBeZipped.add(this.flow);
    previous.add(this);
    for (DataStream d : streams) {
      Objects.requireNonNull(d, NULL_STREAM_MESSAGE);
      toBeZipped.add(d.flow());
      previous.add(d);
    }

    Flowable<Data<Tuple>> flowable = Flowable.zip(toBeZipped, objects -> {
      List<Object> payloads = new ArrayList<>();
      for (Object o : objects) {
        if (!(o instanceof Data)) {
          throw new IllegalArgumentException("Invalid incoming item - " + Data.class.getName() + " expected, received " +
            o.getClass().getName());
        } else {
          payloads.add(((Data) o).payload());
        }
      }
      return new Data<>(Tuple.tuple(payloads.toArray(new Object[payloads.size()])));
    });

    return new DataStreamImpl<>(previous, flowable);
  }


  @Override
  public <OUT> DataStream<OUT> transform(Function<Data<T>, Data<OUT>> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);
    return new DataStreamImpl<>(this, flow.map(function::apply));
  }

  @Override
  public <OUT> DataStream<OUT> transformPayload(Function<T, OUT> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);
    // TODO we are loosing the headers.
    return new DataStreamImpl<>(this,
      flow.map(Data::payload).map(function::apply).map(Data::new));
  }

  @Override
  public <OUT> DataStream<OUT> transformPayloadFlow(Function<Flowable<T>, Flowable<OUT>> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);
    // TODO we are loosing the headers.

    Flowable<Data<OUT>> flowable = function.apply(flow.map(Data::payload)).map(Data::new);
    return new DataStreamImpl<>(this, flowable);
  }

  @Override
  public <OUT> DataStream<OUT> transformFlow(Function<Flowable<Data<T>>, Flowable<Data<OUT>>> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);

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
  public Pair<DataStream<T>, DataStream<T>> branchOnPayload(Predicate<T> condition) {
    DataStream<T> success = new DataStreamImpl<T, T>();
    DataStream<T> failure = new DataStreamImpl<T, T>();

    Branch<T> build = new Branch.BranchBuilder<T>().add(x -> condition.test(x.payload()), success)
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
  public List<DataStream<T>> branchOnPayload(Predicate<T>... conditions) {
    List<DataStream<T>> branches = new ArrayList<>();
    Branch.BranchBuilder<T> builder = new Branch.BranchBuilder<>();
    for (Predicate<T> predicate : conditions) {
      DataStream<T> stream = new DataStreamImpl<T, T>();
      builder.add(x -> predicate.test(x.payload()), stream);
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
    // Update the upstreams.
    upstreams = Collections.singletonList(source);
    this.connector.connectDownstream(source);
  }

  @Override
  public DataStream<T> onData(Consumer<? super Data<T>> consumer) {
    return new DataStreamImpl<>(this, flow.doOnNext(consumer::accept));
  }

  @Override
  public DataStream<T> onPayload(Consumer<? super T> consumer) {
    return new DataStreamImpl<>(this,
      flow.doOnNext(d -> consumer.accept(d.payload())));
  }

  @SuppressWarnings("unchecked")
  public Collection<DataStream> upstreams() {
    return upstreams;
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
