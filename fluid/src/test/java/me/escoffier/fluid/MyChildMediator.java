package me.escoffier.fluid;

import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MyChildMediator extends MyParentMediator {

  @Inbound("input")
  Source<String> source;

  @Inbound("output")
  Sink<String> sink;

  @Override
  public Source<String> source() {
    return source;
  }

  @Override
  public Sink<String> sink() {
    return sink;
  }
}
