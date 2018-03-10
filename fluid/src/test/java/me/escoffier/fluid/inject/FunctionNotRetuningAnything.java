package me.escoffier.fluid.inject;

import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Outbound;
import me.escoffier.fluid.models.Message;
import me.escoffier.fluid.models.Sink;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FunctionNotRetuningAnything {

  @Outbound("my-sink")
  private Sink<String> sink;

  @Function
  public void transform(@Inbound("my-source") String data) {
    sink.dispatch(data.toUpperCase()).subscribe();
  }
}
