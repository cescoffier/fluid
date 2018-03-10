package me.escoffier.fluid.inject;

import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.models.Message;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FunctionGettingMessage {

  @Function(outbound = "my-sink")
  public String transform(@Inbound("my-source") Message<String> data) {
    return data.payload().toUpperCase();
  }
}
