package me.escoffier.fluid.eventbus;

import io.reactivex.Completable;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.EventBus;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.Message;
import me.escoffier.fluid.models.Sink;

/**
 * A sink dispatching messages to the Vert.x event bus.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSink<T> implements Sink<T> {
  private final String name;
  private final String address;
  private final EventBus eventBus;
  private final Boolean publish;

  public EventBusSink(Vertx vertx, Config config) {
    name = config.getString("name").orElse(null);
    address = config.getString("address", name);

    if (address == null) {
      throw new IllegalArgumentException("The name or the address must be set");
    }

    publish = config.getBoolean("publish", true);

    eventBus = vertx.eventBus();
  }

  @Override
  public Completable dispatch(Message<T> message) {
    DeliveryOptions options = new DeliveryOptions();
    message.headers().forEach((k, v) -> options.addHeader(k, v.toString()));

    if (publish) {
      eventBus.publish(address, message.payload(), options);
    } else {
      eventBus.send(address, message, options);
    }
    return Completable.complete();
  }

  @Override
  public String name() {
    return name;
  }
}
