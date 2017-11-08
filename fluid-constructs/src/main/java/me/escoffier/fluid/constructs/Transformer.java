package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface Transformer<IN, OUT> {

    Flowable<OUT> transform(Flowable<IN> flow);

}
