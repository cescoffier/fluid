package me.escoffier.fluid.constructs;

import io.reactivex.Completable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A sink keeping the received items in a list. Do not use this sink on unbounded streams.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ListSink<OUT> implements Sink<OUT> {
  private List<OUT> values = new CopyOnWriteArrayList<>();

  @Override
  public Completable dispatch(OUT data) {
    return Completable.fromAction(() -> values.add(data));
  }

  public synchronized List<OUT> values() {
    return new ArrayList<>(values);
  }
}
