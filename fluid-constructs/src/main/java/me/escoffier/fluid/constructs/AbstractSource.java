package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.plugins.RxJavaPlugins;
import me.escoffier.fluid.constructs.impl.Branch;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;
import me.escoffier.fluid.constructs.impl.FlowPropagation;
import me.escoffier.fluid.constructs.impl.Windows;
import org.reactivestreams.Publisher;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static me.escoffier.fluid.constructs.Pair.pair;

/**
 * Abstract source implementation.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class AbstractSource<T> implements Source<T> {

  static {
    RxJavaPlugins.setOnFlowableSubscribe(new FlowPropagation());
  }

  private final DataStreamImpl<T, T> connector;
  private final Flowable<Data<T>> source;
  private final FlowableTransformer<Data<T>, Flowable<Data<T>>> windowing;

  public AbstractSource(Publisher<Data<T>> flow) {
    this(flow, f -> f.window(1));
  }

  public AbstractSource(Publisher<Data<T>> flow, FlowableTransformer<Data<T>, Flowable<Data<T>>>
    windowing) {
    this.source = Flowable.fromPublisher(flow);
    this.connector = new DataStreamImpl<>(this);
    this.windowing = windowing;
  }

  @Override
  public Source<T> windowBySize(int size) {
    return new AbstractSource<>(this.source, Windows.windowBySize(size));
  }

  @Override
  public Source<T> windowByTime(long duration, TimeUnit unit) {
    return new AbstractSource<>(this.source, Windows.windowByTime(duration, unit));
  }

  @Override
  public Source<T> windowBySizeAndTime(int size, long duration, TimeUnit unit) {
    return new AbstractSource<>(this.source, Windows.windowBySizeOrTime(size, duration, unit));
  }

  private void traverseToFindLasts(DataStream current, Set<DataStream> found) {
    Collection<DataStream> upstreams = current.upstreams();

    if (upstreams.isEmpty()) {
      // Current is the last item.
      found.add(current);
    } else {
      upstreams.forEach(ds -> traverseToFindLasts(ds, found));
    }
  }

  @Override
  public void ready(DataStream first) {

    System.out.println("Ready am I: " + this);
    // Get lasts
    Set<DataStream> lasts = new HashSet<>();
    traverseToFindLasts(connector, lasts);

    System.out.println(this + " Connector is" + connector);
    System.out.println(this + " Last is " + lasts);

    source
      .compose(windowing)
      .doOnNext(win -> {
        // We got a new window

        // Create a new Data Stream, and connect it
        DataStream<T> ds = new DataStreamImpl<T, T>(win);
        connector.connect(ds);

        // For each sink, subscribe.
        lasts.forEach(s -> {
          s.flow().subscribe(
              x -> {},
              System.out::println
          );
          }
        );
      })
      .subscribe(
        d -> {},
        err -> err.printStackTrace()
      );
  }

  @Override
  public Sink<T> to(Sink<T> sink) {
    return connector.to(sink);
  }


  @Override
  public DataStream<T> mergeWith(DataStream<T>... streams) {
    DataStream<T> next = connector.mergeWith(streams);
    connector.setNext(next);
    for (DataStream s : streams) {
      ((DataStreamImpl) s).setNext(connector);
    }
    return next;
  }

  @Override
  public DataStream<T> mergeWith(DataStream<T> stream) {
    DataStream<T> next = connector.mergeWith(stream);
    connector.setNext(next);
    ((DataStreamImpl) stream).setNext(connector);
    return next;
  }

  @Override
  public DataStream<T> concatWith(DataStream<T>... streams) {
    DataStream<T> next = connector.concatWith(streams);
    connector.setNext(next);
    return next;
  }

  @Override
  public DataStream<T> concatWith(DataStream<T> stream) {
    DataStream<T> next = connector.concatWith(stream);
    connector.setNext(next);
    return next;
  }

  @Override
  public <O> DataStream<Pair<T, O>> zipWith(DataStream<O> stream) {
    DataStream<Pair<T, O>> next = connector.zipWith(stream);
    connector.setNext(next);
    return next;
  }

  @Override
  public <O1, O2> DataStream<Tuple> zipWith(DataStream<O1> stream1, DataStream<O2> stream2) {
    DataStream<Tuple> next = connector.zipWith(stream1, stream2);
    connector.setNext(next);
    return next;
  }

  @Override
  public <OUT> DataStream<OUT> transform(Function<Data<T>, Data<OUT>> function) {
    DataStream<OUT> next = connector.transform(function);
    connector.setNext(next);
    return next;
  }

  @Override
  public <OUT> DataStream<OUT> transformPayload(Function<T, OUT> function) {
    DataStream<OUT> next = connector.transformPayload(function);
//    connector.setNext(next);
    System.out.println(this + " Upstream of " + connector + " are " + connector.upstreams());
    return next;
  }

  @Override
  public <OUT> DataStream<OUT> transformPayloadFlow(Function<Flowable<T>, Flowable<OUT>> function) {
    DataStream<OUT> next = connector.transformPayloadFlow(function);
    connector.setNext(next);
    return next;
  }

  @Override
  public <OUT> DataStream<OUT> transformFlow(Function<Flowable<Data<T>>, Flowable<Data<OUT>>> function) {
    DataStream<OUT> next = connector.transformFlow(function);
    connector.setNext(next);
    return next;
  }

  @Override
  public List<DataStream<T>> broadcast(int numberOfBranches) {
    List<DataStream<T>> next = connector.broadcast(numberOfBranches);
    // TODO How to get the data stream here.
    return next;
  }

  @Override
  public Pair<DataStream<T>, DataStream<T>> branch(Predicate<Data<T>> condition) {
    return connector.branch(condition);
    // TODO How to get the data stream here.
  }

  @Override
  public Pair<DataStream<T>, DataStream<T>> branchOnPayload(Predicate<T> condition) {

    DataStreamImpl<T, T> success = new DataStreamImpl<>();
    DataStreamImpl<T, T> failure = new DataStreamImpl<>();

    Branch<T> build = new Branch.BranchBuilder<T>()
      .add(x -> condition.test(x.payload()), success)
      .addFallback(failure).build();
    DataStreamImpl<T, T> stream = new DataStreamImpl<>(connector, build);

    success.connect(stream);
    failure.connect(stream);
    build.connect(connector);

    stream.setNext(success, failure);
    connector.setNext(stream);

    return pair(success, failure);
  }

  @Override
  public List<DataStream<T>> branch(Predicate<Data<T>>... conditions) {
    return connector.branch(conditions);
    // TODO How to get the data stream here.
  }

  @Override
  public List<DataStream<T>> branchOnPayload(Predicate<T>... conditions) {
    return connector.branchOnPayload(conditions);
    // TODO How to get the data stream here.
  }

  @Override
  public void connect(DataStream<T> source) {
    connector.connect(source);
  }

  @Override
  public DataStream<T> onData(Consumer<? super Data<T>> consumer) {
    DataStream<T> next = connector.onData(consumer);
    connector.setNext(next);
    return next;
  }

  @Override
  public DataStream<T> onPayload(Consumer<? super T> consumer) {
    DataStream<T> next = connector.onPayload(consumer);
    connector.setNext(next);
    return next;
  }

  @Override
  public boolean isConnectable() {
    return connector.isConnectable();
  }

  @Override
  public Flowable<Data<T>> flow() {
    return connector.flow();
  }

  @Override
  public Collection<DataStream> downtreams() {
    return connector.downtreams();
  }

  @Override
  public Collection<DataStream> upstreams() {
    return Collections.emptyList();
  }
}
