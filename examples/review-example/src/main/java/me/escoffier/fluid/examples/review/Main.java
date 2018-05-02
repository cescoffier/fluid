package me.escoffier.fluid.examples.review;


import me.escoffier.fluid.framework.Fluid;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Main {

  public static void main(String[] args) {
    Fluid.create()
      .deploy(ReviewProducer.class)
      .deploy(ReviewGlobalRating.class)
      .deploy(FraudDetector.class)
      .deploy(ReviewRecentRating.class);
  }
}
