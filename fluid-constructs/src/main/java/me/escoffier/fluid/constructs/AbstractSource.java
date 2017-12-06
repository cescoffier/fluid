package me.escoffier.fluid.constructs;

import io.reactivex.plugins.RxJavaPlugins;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;
import me.escoffier.fluid.constructs.impl.FlowPropagation;
import me.escoffier.fluid.constructs.impl.Windows;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * Abstract source implementation.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class AbstractSource<T> extends DataStreamImpl<Void, T> implements Source<T> {

  static {
    RxJavaPlugins.setOnFlowableSubscribe(new FlowPropagation());
  }

  public AbstractSource(Publisher<Data<T>> flow) {
    super(flow);
  }


  @Override
  public Source<T> windowBySize(int size) {
    return new AbstractSource<>(Windows.windowBySize(flow, size));
  }

  @Override
  public Source<T> windowByTime(long duration, TimeUnit unit) {
    return new AbstractSource<>(Windows.windowByTime(flow, duration, unit));
  }

  @Override
  public Source<T> windowBySizeAndTime(int size, long duration, TimeUnit unit) {
    return new AbstractSource<>(Windows.windowBySizeOrTime(flow, size, duration, unit));
  }
}
