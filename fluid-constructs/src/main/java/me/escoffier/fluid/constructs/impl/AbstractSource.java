package me.escoffier.fluid.constructs.impl;

import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.Source;
import org.reactivestreams.Publisher;

import java.util.Collections;

/**
 * Default source.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class AbstractSource<T> extends DataStreamImpl<Void, T> implements Source<T> {
  public AbstractSource(Publisher<Data<T>> flow) {
    super(Collections.emptyList(), flow);
  }


}
