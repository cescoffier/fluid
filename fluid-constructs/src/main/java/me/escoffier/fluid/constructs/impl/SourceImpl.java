package me.escoffier.fluid.constructs.impl;

import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.Source;
import org.reactivestreams.Publisher;

/**
 * Default source.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SourceImpl<T> extends DataStreamImpl<Void, T> implements Source<T> {
  public SourceImpl(Publisher<Data<T>> flow) {
    super(null, flow);
  }


}
