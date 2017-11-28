package me.escoffier.fluid.api.components;

import io.reactivex.Flowable;
import me.escoffier.fluid.api.OutPort;
import me.escoffier.fluid.api.SourceComponent;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Source {

  static <T> SourceComponent<T> from(T... items) {
    return new SourceComponent<T>() {
      private Output<T> output = new Output<>("source");

      @Override
      public OutPort<T> output() {
        return output;
      }

      @Override
      public void onCreation() {
        // Do nothing.
      }

      @Override
      public void onAssembly() {
        output.connect(Flowable.fromArray(items));
      }
    };
  }

}
