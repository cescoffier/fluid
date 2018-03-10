package me.escoffier.fluid.inject;

import io.reactivex.Flowable;
import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.models.Message;
import org.reactivestreams.Publisher;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FunctionReturningPublisher {

  @Function(outbound = "my-sink")
  public Publisher<String> transform(@Inbound("my-source") Message<String> data) {
    return Flowable.fromArray(data.payload(), data.payload().toUpperCase());
  }
}
