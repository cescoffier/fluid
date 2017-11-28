package me.escoffier.fluid.api.components;

import me.escoffier.fluid.api.Component;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Draft {


  public static void main(String[] args) {
    Component c = Source.from("a", "b", "c")
      .connectTo(Sink.forEach(t -> System.out.println(">> " + t)));

    c.run();

  }

}
