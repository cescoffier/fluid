package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;
import me.escoffier.fluid.constructs.FlowContext;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
@SuppressWarnings("rawtypes")
public class FlowPropagation implements BiFunction<Flowable, Subscriber, Subscriber> {


  @Override
  @SuppressWarnings("unchecked")
  public Subscriber apply(Flowable flowable, Subscriber subscriber) throws Exception {
    return new FlowPropagationSubscriber<>(subscriber);
  }

  private class FlowPropagationSubscriber<T> implements Subscriber<T> {
    private final Subscriber<T> actual;
    private Map<String, Object> context = new HashMap<>();

    FlowPropagationSubscriber(Subscriber<T> subscriber) {
      this.actual = subscriber;
    }

    @Override
    public void onSubscribe(Subscription s) {
      FlowContext.pushContext(context);
      actual.onSubscribe(s);
      FlowContext.popContext();
    }

    @Override
    public void onNext(T o) {
      FlowContext.pushContext(context);
      actual.onNext(o);
      FlowContext.popContext();
    }

    @Override
    public void onError(Throwable t) {
      FlowContext.pushContext(context);
      actual.onError(t);
      FlowContext.popContext();
    }

    @Override
    public void onComplete() {
      FlowContext.pushContext(context);
      actual.onComplete();
      FlowContext.popContext();
    }
  }
}
