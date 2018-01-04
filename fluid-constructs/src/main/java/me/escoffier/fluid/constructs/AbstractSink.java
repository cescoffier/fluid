package me.escoffier.fluid.constructs;

import io.reactivex.Completable;
import io.reactivex.Flowable;

import java.util.*;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public abstract class AbstractSink<T> implements Sink<T> {

  // TODO Sink may produce results (accumulation...), they should be exposed as Flowable<X>.

  private final boolean dispatchOnData;
  private final Map<Window<T>, List<Data<T>>> storage = new HashMap<>();

  public AbstractSink() {
    this.dispatchOnData = true;
  }

  public AbstractSink(boolean dispatchOnWatermark) {
    this.dispatchOnData = ! dispatchOnWatermark;
  }

  public Completable dispatch(Data<T> data) {
      if (dispatchOnData) {
        return Completable.fromAction(() -> process(data));
      } else {
        Window<T> window = data.get("fluid-window");
        if (Watermark.isWatermark(data)) {
          return Flowable.fromIterable(getAndClear(window))
            .flatMapCompletable(d -> Completable.fromAction(() -> process(d)));
        } else {
          addToStorage(window, data);
          return Completable.complete();
        }
      }
  }

  synchronized void addToStorage(Window<T> window, Data<T> data) {
    Objects.requireNonNull(window);
    Objects.requireNonNull(data);
    List<Data<T>> list = storage.computeIfAbsent(window, (w) -> new ArrayList<>());
    list.add(data);
  }

  protected synchronized List<Data<T>> getAndClear(Window<T> window) {
    Objects.requireNonNull(window);
    List<Data<T>> list = storage.remove(window);
    if (list == null) {
      return Collections.emptyList();
    }
    return list;
  }

  public abstract void process(Data<T> data);

}
