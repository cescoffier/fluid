package me.escoffier.fluid.constructs;

import io.reactivex.Completable;

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
  private final List<Data<OUT>> values = new CopyOnWriteArrayList<>();
  private final boolean dispatchOnWatermark;

  public ListSink() {
    this.dispatchOnWatermark = false;
  }

  public ListSink(boolean dispatchOnWatermark) {
    this.dispatchOnWatermark = dispatchOnWatermark;
  }

  @Override
  public Completable dispatch(Data<OUT> data) {
    return Completable.fromAction(() -> {
      if (!(ControlData.isControl(data))) {
        values.add(data);
      }
    });
  }

  public synchronized List<OUT> values() {
    return values.stream().map(Data::payload).collect(Collectors.toList());
  }

  public synchronized List<Data<OUT>> data() {
    return new ArrayList<>(values);
  }
}
