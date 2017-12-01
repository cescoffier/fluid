package me.escoffier.fluid.constructs;

import io.reactivex.Completable;

import java.util.function.BiFunction;

/**
 * A sink applying a function on each received payload. The function receives the last computed value and the received
 * payload.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ScanSink<OUT, RES> implements Sink<OUT> {
  private final BiFunction<OUT, RES, RES> mapper;
  private RES current;

  // TODO provide a flowable to collect the "current" values.

  ScanSink(RES init, BiFunction<OUT, RES, RES> mapper) {
    this.mapper = mapper;
    this.current = init;
  }

  @Override
  public synchronized Completable dispatch(Data<OUT> data) {
    return Completable.fromAction(() -> {
      synchronized (me.escoffier.fluid.constructs.ScanSink.this) {
        current = mapper.apply(data.payload(), current);
      }
    });

  }

  /**
   * @return the current value.
   */
  public synchronized RES value() {
    return current;
  }
}
