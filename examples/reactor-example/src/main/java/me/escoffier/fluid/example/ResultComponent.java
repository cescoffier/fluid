package me.escoffier.fluid.example;

import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Inbound;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ResultComponent {

  @Function(outbound = "res")
  public String printResult(@Inbound("data") int val) {
      return process(val);
  }

  private String process(int val) {
    return "";
  }
}
