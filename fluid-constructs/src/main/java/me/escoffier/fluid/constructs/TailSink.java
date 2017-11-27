package me.escoffier.fluid.constructs;

import io.reactivex.Completable;
import io.reactivex.Flowable;

import java.util.Optional;

/**
 * A sink allowing to retrieve the last item received by the sink. Items received before are discarded.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class TailSink<OUT> implements Sink<OUT> {

  /**
   * The last received value.
   */
  private OUT tail;

  @Override
  public Completable dispatch(OUT data) {
    return Completable.fromAction(() -> {
      synchronized (TailSink.this) {
        tail = data;
      }
    });
  }

  /**
   * @return the stored value.
   */
  public synchronized OUT value() {
    return tail;
  }

  /**
   * @return an optional encapsulating the stored value. Be aware that the optional is created at call time, and so
   * the value won't change even.
   */
  public synchronized Optional<OUT> optional() {
    return Optional.ofNullable(tail);
  }
}
