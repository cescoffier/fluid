package me.escoffier.fluid.inject;

import io.reactivex.Flowable;
import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.models.Message;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FunctionReturningFlowable {

  @Function(outbound = "my-sink")
  public Flowable<String> transform(@Inbound("my-source") Message<String> data) {
    return Flowable.fromArray(data.payload(), data.payload().toUpperCase());
  }
}
