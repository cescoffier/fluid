package me.escoffier.fluid.impl;

import io.reactivex.Completable;
import me.escoffier.fluid.models.Message;
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
  private List<Message<OUT>> values = new CopyOnWriteArrayList<>();

  @Override
  public Completable dispatch(Message<OUT> message) {
    return Completable.fromAction(() -> values.add(message));
  }

  public synchronized List<OUT> values() {
    return values.stream().map(Message::payload)
      .collect(Collectors.toList());
  }

  public synchronized List<Message<OUT>> data() {
    return new ArrayList<>(values);
  }
}
