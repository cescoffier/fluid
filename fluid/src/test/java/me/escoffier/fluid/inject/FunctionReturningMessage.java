package me.escoffier.fluid.inject;

import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.models.Message;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FunctionReturningMessage {

  @Function(outbound = "my-sink")
  public Message<String> transform(@Inbound("my-source") Message<String> data) {
    return new Message<>(data.payload().toUpperCase()).with("X-header", "X-value");
  }
}
