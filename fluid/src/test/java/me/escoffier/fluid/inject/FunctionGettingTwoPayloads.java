package me.escoffier.fluid.inject;

import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.models.Message;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FunctionGettingTwoPayloads {

  @Function(outbound = "my-sink")
  public String transform(@Inbound("my-source") String d1, @Inbound("my-source") String d2) {
    return d1.toUpperCase() + d2.toUpperCase();
  }
}
