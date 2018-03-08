package me.escoffier.fluid.impl;

import io.reactivex.Completable;
import me.escoffier.fluid.models.Data;
import me.escoffier.fluid.models.Sink;

/**
 * A sink storing the first received value, and discarding all the other ones.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class HeadSink<OUT> implements Sink<OUT> {
  private OUT head;

  @Override
  public Completable dispatch(Data<OUT> data) {
    return Completable.fromAction(() -> {
      synchronized (HeadSink.this) {
        if (head == null) {
          // TODO It would be nice to be able to cancel the subscription
          head = data.payload();
        }
      }
    });
  }

  public synchronized OUT value() {
    return head;
  }
}
