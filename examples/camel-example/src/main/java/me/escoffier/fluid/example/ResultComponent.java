package me.escoffier.fluid.example;

import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Inbound;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ResultComponent {

  @Function
  public void printResult(@Inbound("result") int val) {
    System.out.println("Result: " + val);
  }
}
