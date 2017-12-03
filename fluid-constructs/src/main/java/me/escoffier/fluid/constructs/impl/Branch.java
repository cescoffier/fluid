package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.DataStream;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Branch<IN> implements Processor<Data<IN>, Data<IN>> {

  private List<BranchLogic<IN>> branches = new CopyOnWriteArrayList<>();

  private Flowable<Data<IN>> source;

  private AtomicInteger subscriptionCount = new AtomicInteger();

  private Branch(List<BranchLogic<IN>> branches) {
    this.branches.addAll(branches);
  }


  public synchronized void connect(DataStream<IN> src) {
    if (source != null) {
      throw new IllegalStateException("Connectable stream already connected");
    } else {
      source = src.flow();
    }
  }

  @Override
  public void subscribe(Subscriber<? super Data<IN>> s) {
    synchronized (this) {
      if (source == null) {
        s.onError(new Exception("Connectable stream not connected"));
        return;
      }
    }
    if (subscriptionCount.incrementAndGet() == branches.size()) {
      source.subscribe(this);
    }
  }

  @Override
  public void onSubscribe(Subscription s) {
    for (BranchLogic branch : branches) {
      //TODO Not sure we can pass this subscription like this
      //TODO it may let us implement back-pressure correctly.
      ((DataStreamImpl<IN, IN>) branch.output).connector().onSubscribe(s);
    }
  }

  @Override
  public void onNext(Data<IN> s) {
    for (BranchLogic branch : branches) {
      if (branch.acceptAndDispatch(s)) {
        return;
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    branches.forEach(b -> b.propagateError(t));
  }

  @Override
  public void onComplete() {
    branches.forEach(BranchLogic::propagateCompletion);
  }

  public static class BranchBuilder<IN> {

    private List<BranchLogic<IN>> branches = new ArrayList<>();

    BranchBuilder<IN> add(Predicate<Data<IN>> predicate, DataStream<IN> stream) {
      branches.add(new BranchLogic<>(predicate, stream));
      return this;
    }

    BranchBuilder<IN> addFallback(DataStream<IN> stream) {
      branches.add(new BranchLogic<>((x) -> true, stream));
      return this;
    }

    Branch<IN> build() {
      return new me.escoffier.fluid.constructs.impl.Branch<>(branches);
    }

  }

  private static class BranchLogic<IN> {

    private final Predicate<Data<IN>> filter;

    private final DataStream<IN> output;

    public BranchLogic(Predicate<Data<IN>> filter, DataStream<IN> output) {
      this.filter = Objects.requireNonNull(filter);
      this.output = Objects.requireNonNull(output);
    }

    boolean accept(Data<IN> data) {
      return filter.test(data);
    }

    boolean acceptAndDispatch(Data<IN> data) {
      if (accept(data)) {
        // TODO What is we don't have a connector...
        ((DataStreamImpl<IN, IN>) output).connector().onNext(data);
        return true;
      }
      return false;
    }

    public void propagateError(Throwable err) {
      ((DataStreamImpl<IN, IN>) output).connector().onError(err);
    }


    public void propagateCompletion() {
      ((DataStreamImpl<IN, IN>) output).connector().onComplete();
    }
  }

}
