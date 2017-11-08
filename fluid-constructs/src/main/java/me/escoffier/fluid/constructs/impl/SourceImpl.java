package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.DataStream;
import me.escoffier.fluid.constructs.Source;
import org.reactivestreams.Publisher;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SourceImpl<T> extends DataStreamImpl<Void, T> implements Source<T> {
    public SourceImpl(Publisher<T> flow) {
        super(null, flow);
    }
}
