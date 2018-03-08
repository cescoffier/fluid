package me.escoffier.fluid.inject;

import io.reactivex.Flowable;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Outbound;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MediatorRequiringSink {

  @Inbound("my-source")
  private Flowable<String> source;

  @Outbound("my-sink")
  private Sink<String> sink;


  @Transformation
  public void transform() {
    source
      .map(String::toUpperCase)
      .flatMapCompletable(sink::dispatch)
      .subscribe();
  }
}
