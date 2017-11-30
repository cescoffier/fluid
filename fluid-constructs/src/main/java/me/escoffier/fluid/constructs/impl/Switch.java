package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.DataStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Switch<IN> {

  private List<Branch<IN>> branches = new CopyOnWriteArrayList<>();

  public Switch(List<Branch<IN>> branches) {
    this.branches.addAll(branches);
  }

  public static class SwitchBuilder<T> {

    private List<Branch<T>> branches = new ArrayList<>();

    SwitchBuilder add(Predicate<Data<T>> predicate, DataStream<T> stream) {
      branches.add(new Branch<>(predicate, stream));
      return this;
    }

    Switch<T> build() {
      return new Switch<>(branches);
    }

  }

  public void connectToUpstream(Flowable<Data<IN>> flowable) {
    // TODO This does not enforce the reactive stream back pressure.
    flowable
      .doOnError(err -> {
            branches.forEach(b -> b.propagateError(err));
      })
      .doOnNext(data -> {
        for (Branch<IN> branch : branches) {
          if (branch.acceptAndDispatch(data)) {
            return;
          }
        }
      }
    );
  }

  private static class Branch<T> {

    private final Predicate<Data<T>> filter;

    private final DataStream<T> output;

    public Branch(Predicate<Data<T>> filter, DataStream<T> output) {
      this.filter = Objects.requireNonNull(filter);
      this.output = Objects.requireNonNull(output);


    }

    boolean accept(Data<T> data) {
      return filter.test(data);
    }

    boolean acceptAndDispatch(Data<T> data) {
      if (accept(data)) {
        // TODO What is we don't have a connector...
        output.connector().onNext(data);
        return true;
      }
      return false;
    }

    public void propagateError(Throwable err) {
      output.connector().onError(err);
    }


  }

}
