package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.*;
import org.reactivestreams.Publisher;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
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
  private Collection<DataStream> downstreams = new CopyOnWriteArraySet<>();

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
  public synchronized void setDownstreams(DataStream... next) {
    downstreams.addAll(Arrays.asList(next));
  }


  @Override
  @SafeVarargs
  public final DataStream<T> mergeWith(DataStream<T>... streams) {
    Objects.requireNonNull(streams, NULL_STREAMS_MESSAGE);

    List<Flowable<Data<T>>> toBeMerged = new ArrayList<>();
    toBeMerged.add(this.flow);
    for (DataStream<T> stream : streams) {
      toBeMerged.add(stream.flow().filter(d -> !ControlData.isControl(d)));
    }

    Flowable<Data<T>> merged = Flowable.merge(toBeMerged);

    List<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.addAll(Arrays.asList(streams));

    DataStreamImpl<Object, T> next = new DataStreamImpl<>(previous, merged);
    this.setDownstreams(next);
    Arrays.stream(streams).forEach(d -> d.setDownstreams(next));

    return next;
  }

  @SuppressWarnings("unchecked")
  @Override
  public final DataStream<T> mergeWith(DataStream<T> stream) {
    DataStream[] array = {stream};
    return mergeWith(array);
  }

  @Override
  @SafeVarargs
  @Deprecated
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

    DataStreamImpl<Object, T> next = new DataStreamImpl<>(previous, merged);
    this.setDownstreams(next);
    Arrays.stream(streams).forEach(d -> d.setDownstreams(next));

    return next;
  }

  @SuppressWarnings("unchecked")
  @Override
  @Deprecated
  public final DataStream<T> concatWith(DataStream<T> stream) {
    DataStream[] array = {stream};
    return concatWith(array);
  }

  @Override
  public <O> DataStream<Pair<T, O>> zipWith(DataStream<O> stream) {
    Objects.requireNonNull(stream, NULL_STREAM_MESSAGE);

    Flowable<Data<Pair<T, O>>> fusion =
      flow.groupBy(ControlData::isControl)
        .flatMap(f -> {
          if (f.getKey()) {
            return f.<Data<Pair<T, O>>>map(ControlData.class::cast);
          } else {
            return f.zipWith(stream.flow().filter(d -> !ControlData.isControl(d)),
              (a, b) -> a.with(pair(a.payload(), b.payload())));
          }
        });

    List<DataStream> previous = new ArrayList<>();
    previous.add(this);
    previous.add(stream);

    DataStreamImpl<Object, Pair<T, O>> next = new DataStreamImpl<>(previous, fusion);
    this.setDownstreams(next);
    stream.setDownstreams(next);

    return next;
  }

  @Override
  public DataStream<Tuple> zipWith(DataStream... streams) {
    Objects.requireNonNull(streams, NULL_STREAM_MESSAGE);

    List<DataStream> previous = new ArrayList<>();
    previous.add(this);
    for (DataStream d : streams) {
      Objects.requireNonNull(d, NULL_STREAM_MESSAGE);
      previous.add(d);
    }

    Flowable<Data<Tuple>> fusion =
      flow.groupBy(ControlData::isControl)
        .flatMap(f -> {
          if (f.getKey()) {
            return f.<Data<Tuple>>map(ControlData.class::cast);
          } else {
            List<Flowable<?>> toBeZipped = new ArrayList<>();
            toBeZipped.add(f);
            for (DataStream d : streams) {
              toBeZipped.add(d.flow().filter(ControlData::isNotControl));
            }
            return Flowable.zip(toBeZipped, objects -> {
              List<Object> payloads = new ArrayList<>();
              Data<?> first = null;
              for (Object o : objects) {
                if (!(o instanceof Data)) {
                  throw new IllegalArgumentException("Invalid incoming item - "
                    + Data.class.getName() + " expected, received " +
                    o.getClass().getName());
                } else {
                  if (first == null) {
                    first = ((Data) o);
                  }
                  payloads.add(((Data) o).payload());
                }
              }
              if (first == null) {
                throw new IllegalStateException("Invalid set of stream");
              }
              return first.with(Tuple.tuple(payloads.toArray(new Object[payloads.size()])));
            });
          }
        });

    DataStreamImpl<Object, Tuple> next = new DataStreamImpl<>(previous, fusion);
    this.setDownstreams(next);
    Arrays.stream(streams).forEach(d -> d.setDownstreams(next));
    return next;
  }


  @Override
  public <OUT> DataStream<OUT> transform(Function<Data<T>, Data<OUT>> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);
    DataStreamImpl<T, OUT> next = new DataStreamImpl<>(this, flow.map(d -> {
      if (ControlData.isControl(d)) {
        return (ControlData) d;
      }
      return function.apply(d);
    }));
    this.setDownstreams(next);
    return next;
  }

  @Override
  public <OUT> DataStream<OUT> transform(Function<Data<T>, Data<OUT>> function, boolean includeControl) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);
    if (includeControl) {
      DataStreamImpl<T, OUT> next = new DataStreamImpl<>(this, flow.map(function::apply));
      this.setDownstreams(next);
      return next;
    } else {
      return transform(function);
    }
  }

  @Override
  public <OUT> DataStream<OUT> transformPayload(Function<T, OUT> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);
    DataStreamImpl<T, OUT> next = new DataStreamImpl<>(this,
      flow
        .flatMap(data -> {
          if (!ControlData.isControl(data)) {
            return Flowable.just(data).map(Data::payload).map(function::apply).map(data::with);
          } else {
            ControlData<OUT> control = (ControlData) data;
            return Flowable.just(control);
          }
        })
    );
    this.setDownstreams(next);
    return next;
  }

  @Override
  public <OUT> DataStream<OUT> transformPayloadFlow(Function<Flowable<T>, Flowable<OUT>> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);

    Flowable<Data<OUT>> fusion =
      flow.groupBy(ControlData::isControl)
        .flatMap(f -> {
          if (f.getKey()) {
            return f.<Data<OUT>>map(ControlData.class::cast);
          } else {
            return f.compose(upstream -> Flowable.defer(() -> {
              AtomicReference<Data<T>> current = new AtomicReference<>();
              Flowable<T> flowable = upstream.doOnNext(current::set).map(Data::payload);
              return function.apply(flowable)
                .map(s -> {
                  if (current.get() == null) {
                    // The famous "scan" case where an item can be emitted without passing in the upstream processing, as
                    // it emits the root value.
                    return new Data<>(s);
                  }
                  return current.get().with(s);
                });
            }));
          }
        });

    DataStreamImpl<T, OUT> next = new DataStreamImpl<>(this, fusion);
    this.setDownstreams(next);
    return next;
  }

  @Override
  public <OUT> DataStream<OUT> transformFlow(Function<Flowable<Data<T>>, Flowable<Data<OUT>>> function) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);

    Flowable<Data<OUT>> fusion =
      flow.groupBy(ControlData::isControl)
        .flatMap(f -> {
          if (f.getKey()) {
            return f.<Data<OUT>>map(x -> (ControlData) x);
          } else {
            return function.apply(f);
          }
        });

    DataStreamImpl<T, OUT> next = new DataStreamImpl<>(this, fusion);
    this.setDownstreams(next);
    return next;

  }

  @Override
  public <OUT> DataStream<OUT> transformFlow(Function<Flowable<Data<T>>, Flowable<Data<OUT>>> function, boolean
    includeControlData) {
    Objects.requireNonNull(function, NULL_FUNCTION_MESSAGE);
    if (!includeControlData) {
      return transformFlow(function);
    } else {
      Flowable<Data<OUT>> fusion = function.apply(flow);
      DataStreamImpl<T, OUT> next = new DataStreamImpl<>(this, fusion);
      this.setDownstreams(next);
      return next;
    }
  }

  @Override
  public List<DataStream<T>> broadcast(int numberOfBranches) {
    if (numberOfBranches <= 1) {
      throw new IllegalArgumentException("The number of branch must be at least 2");
    }

    List<DataStream<T>> streams = new ArrayList<>(numberOfBranches);
    Flowable<Data<T>> publish = flow.publish().autoConnect(numberOfBranches);
    DataStreamImpl<T, T> stream = new DataStreamImpl<>(this, publish);

    List<DataStream> list = new ArrayList<>();
    for (int i = 0; i < numberOfBranches; i++) {
      DataStream<T> branch = new DataStreamImpl<>();
      streams.add(branch);
      branch.connect(stream);
      list.add(branch);
    }

    stream.setDownstreams(list.toArray(new DataStream[list.size()]));
    this.setDownstreams(stream);

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

    stream.setDownstreams(success, failure);
    this.setDownstreams(stream);

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

    stream.setDownstreams(success, failure);
    this.setDownstreams(stream);

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
    List<DataStream> list = new ArrayList<>();
    for (DataStream<T> b : branches) {
      b.connect(stream);
      list.add(b);
    }
    built.connect(this);

    stream.setDownstreams(list.toArray(new DataStream[list.size()]));
    this.setDownstreams(stream);

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
    List<DataStream> list = new ArrayList<>();
    DataStream<T> stream = new DataStreamImpl<>(this, built);
    for (DataStream<T> b : branches) {
      b.connect(stream);
      list.add(b);
    }
    built.connect(this);

    stream.setDownstreams(list.toArray(new DataStream[list.size()]));
    this.setDownstreams(stream);
    return branches;
  }

  public void connect(DataStream<T> source) {
    // Update the upstreams.
    upstreams = Collections.singletonList(source);
    this.connector.connectDownstream(source);
  }

  @Override
  public DataStream<T> onData(Consumer<? super Data<T>> consumer) {
    DataStreamImpl<T, T> next = new DataStreamImpl<>(this, flow.doOnNext(consumer::accept));
    this.setDownstreams(next);
    return next;
  }

  @Override
  public DataStream<T> onData(Consumer<? super Data<T>> consumer, boolean includeControlData) {
    if (includeControlData) {
      return onData(consumer);
    } else {
      DataStreamImpl<T, T> next = new DataStreamImpl<>(this, flow.doOnNext(d -> {
        if (!d.isControl()) {
          consumer.accept(d);
        }
      }));
      this.setDownstreams(next);
      return next;
    }
  }

  @Override
  public DataStream<T> onPayload(Consumer<? super T> consumer) {
    DataStreamImpl<T, T> next = new DataStreamImpl<>(this,
      flow.doOnNext(d -> {
        if (!d.isControl()) {
          consumer.accept(d.payload());
        }
      }));
    this.setDownstreams(next);
    return next;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<DataStream> upstreams() {
    return upstreams;
  }

  @Override
  public Collection<DataStream> downstreams() {
    return downstreams;
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
    Flowable<Data<Void>> flowable = flow
      .flatMapCompletable(sink::dispatch)
      .doOnError(Throwable::printStackTrace)
      .toFlowable();

    DataStreamImpl<T, Void> last = new DataStreamImpl<>(
      this,
      flowable
    );

    this.setDownstreams(last);

    flowable.subscribe();
    return sink;
  }

  StreamConnector<T> connector() {
    return connector;
  }
}
