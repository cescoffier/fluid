package me.escoffier.fluid.api;

import io.reactivex.Flowable;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface Port<T> {

  String name();

  Class<T> type();

  void connect(Flowable<T> flow);

  Flowable<T> flow();
}
