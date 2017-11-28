package me.escoffier.fluid.api.components;

import me.escoffier.fluid.api.InPort;
import me.escoffier.fluid.api.SinkComponent;

import java.util.function.Consumer;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Sink {

  static <T> SinkComponent<T> forEach(Consumer<T> consumer) {
    return new SinkComponent<T>() {
      private Input<T> input = new Input<>("sink");

      @Override
      public InPort<T> input() {
        return input;
      }

      @Override
      public void onCreation() {
        // Do nothing.
      }

      @Override
      public void onAssembly() {
        input.flow().doOnNext(consumer::accept);
      }
    };
  }

}
