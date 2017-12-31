package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Window<T> {

  private List<Data<T>> data;

  public Window(Collection<Data<T>> items) {
    data = Collections.unmodifiableList(items.stream().map(d -> d.with("fluid-window", this))
      .collect(Collectors.toList())
    );
  }

  public List<Data<T>> data() {
    return data;
  }

  public Flowable<Data<T>> flow() {
    return Flowable.fromIterable(data);
  }

}
