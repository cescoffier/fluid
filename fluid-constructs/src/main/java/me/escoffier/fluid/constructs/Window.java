package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Window<T> {

  private final Flowable<Data<T>> flow;
  private List<Data<T>> data;

  public Window(Collection<Data<T>> items) {
    data = Collections.unmodifiableList(items.stream().map(d -> d.with("fluid-window", this))
      .collect(Collectors.toList())
    );
    flow = null;
  }

  public Window(Flowable<Data<T>> items) {
    this.data = new CopyOnWriteArrayList<>();
    this.flow = Objects.requireNonNull(items).map(d -> {
      Data<T> u = d.with("fluid-window", this);
      data.add(u);
      return u;
    });
  }

  public List<Data<T>> data() {
    return data;
  }

  public Flowable<Data<T>> flow() {
    if (flow != null) {
      return flow;
    } else {
      return Flowable.fromIterable(data);
    }
  }

}
