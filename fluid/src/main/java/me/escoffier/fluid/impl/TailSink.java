package me.escoffier.fluid.impl;

import io.reactivex.Completable;
import me.escoffier.fluid.models.Message;
import me.escoffier.fluid.models.Sink;

import java.util.Optional;

/**
 * A sink allowing to retrieve the last payload received by the sink. Payloads received before are discarded.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class TailSink<OUT> implements Sink<OUT> {

  /**
   * The last received value.
   */
  private Message<OUT> tail;

  @Override
  public Completable dispatch(Message<OUT> message) {
    return Completable.fromAction(() -> {
      synchronized (TailSink.this) {
        tail = message;
      }
    });
  }

  /**
   * @return the stored payload.
   */
  public synchronized OUT value() {
    return Optional.ofNullable(tail).map(Message::payload).orElse(null);
  }

  /**
   * @return the stored data
   */
  public synchronized Message<OUT> data() {
    return tail;
  }

  /**
   * @return an optional encapsulating the stored value. Be aware that the optional is created at call time, and so
   * the value won't change even.
   */
  public synchronized Optional<OUT> optional() {
    return Optional.ofNullable(tail).map(Message::payload);
  }
}
