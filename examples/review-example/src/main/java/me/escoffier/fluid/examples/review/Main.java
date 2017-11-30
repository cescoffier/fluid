package me.escoffier.fluid.examples.review;

import me.escoffier.fluid.Fluid;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Main {

  public static void main(String[] args) {
    Fluid fluid = new Fluid();
    fluid.deploy(ReviewProducer.class);


  }
}
