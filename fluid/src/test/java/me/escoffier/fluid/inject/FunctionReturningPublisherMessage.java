package me.escoffier.fluid.inject;

import io.reactivex.Flowable;
import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.models.Message;
import org.reactivestreams.Publisher;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FunctionReturningPublisherMessage {

  @Function(outbound = "my-sink")
  public Publisher<Message<String>> transform(@Inbound("my-source") Message<String> data) {
    return Flowable.just(new Message<>(data.payload().toUpperCase()).with("X-header", "X-value"));
  }
}
