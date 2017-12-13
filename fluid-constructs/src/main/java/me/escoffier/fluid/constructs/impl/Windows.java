package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.schedulers.Schedulers;
import io.vertx.reactivex.RxHelper;
import io.vertx.reactivex.core.Context;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.FlowContext;

import java.util.concurrent.TimeUnit;

/**
 * Provides transformation to implement windows.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Windows {

  public static final String WINDOW_DATA = "window-data-list";

  /*
       f.toList()
            .doOnSuccess(list -> FlowContext.set(WINDOW_DATA, list))
            .flatMapPublisher(Flowable::fromIterable));
   */

  public static <T> FlowableTransformer<Data<T>, Flowable<Data<T>>> windowBySize(int size) {
    return upstream -> upstream.window(size);
  }

  public static <T> FlowableTransformer<Data<T>, Flowable<Data<T>>> windowByTime(long time, TimeUnit unit) {
    return upstream -> upstream.window(time, unit);
  }

  public static <T> FlowableTransformer<Data<T>, Flowable<Data<T>>> windowBySizeOrTime(int size, long time, TimeUnit unit) {
    Context context = Vertx.currentContext();
    return upstream -> upstream.window(time, unit,
      (context != null ? RxHelper.scheduler(context.getDelegate()) : Schedulers.computation()),
      size,
      true);
  }
}
