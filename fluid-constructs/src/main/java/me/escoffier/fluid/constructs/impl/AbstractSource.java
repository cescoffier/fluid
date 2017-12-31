package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.constructs.Watermark;
import me.escoffier.fluid.constructs.WindowOperator;
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

  @Override
  public Source<T> withWindow(WindowOperator<T> operator) {
    return new AbstractSource<>(
      this.flow.compose(f -> operator.window(f)
        .flatMap(win -> win
          .flow().concatWith(Flowable.just(new Watermark<>(win)))))
    );
  }
}
