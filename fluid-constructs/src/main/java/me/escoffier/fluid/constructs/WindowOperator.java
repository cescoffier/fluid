package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface WindowOperator<T> {

  Flowable<Window<T>> window(Flowable<Data<T>> source);



}
