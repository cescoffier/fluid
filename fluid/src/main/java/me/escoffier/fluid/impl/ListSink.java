package me.escoffier.fluid.impl;

import io.reactivex.Completable;
import me.escoffier.fluid.models.Data;
import me.escoffier.fluid.models.Sink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * A sink keeping the received data in a list. Do not use this sink on unbounded streams.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ListSink<OUT> implements Sink<OUT> {
  private List<Data<OUT>> values = new CopyOnWriteArrayList<>();

  @Override
  public Completable dispatch(Data<OUT> data) {
    return Completable.fromAction(() ->
      values.add(data));
  }

  public synchronized List<OUT> values() {
    return values.stream().map(Data::payload).collect(Collectors.toList());
  }

  public synchronized List<Data<OUT>> data() {
    return new ArrayList<>(values);
  }
}
