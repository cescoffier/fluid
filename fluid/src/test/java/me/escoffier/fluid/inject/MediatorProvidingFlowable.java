package me.escoffier.fluid.inject;

import io.reactivex.Flowable;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Outbound;
import me.escoffier.fluid.annotations.Transformation;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MediatorProvidingFlowable {

  @Inbound("my-source")
  private Flowable<String> source;

  @Transformation
  @Outbound("my-sink")
  public Flowable<String> transform() {
    return source.map(String::toUpperCase).flatMap(s -> Flowable.fromArray(s, s));
  }
}
