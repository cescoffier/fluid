package me.escoffier.fluid.eventbus;

import io.reactivex.Completable;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.EventBus;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.Sink;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSink<T> implements Sink<T> {
  private final String name;
  private final String address;
  private final EventBus eventBus;
  private final Boolean publish;

  public EventBusSink(Vertx vertx, JsonObject json) {
    name = json.getString("name");
    address = json.getString("address", name);

    if (address == null) {
      throw new IllegalArgumentException("The name or the address must be set");
    }

    publish = json.getBoolean("publish", true);

    eventBus = vertx.eventBus();
  }

  @Override
  public Completable dispatch(Data<T> data) {
    DeliveryOptions options = new DeliveryOptions();
    data.headers().forEach((k, v) -> options.addHeader(k, v.toString()));

    if (publish) {
      eventBus.publish(address, data.item(), options);
    } else {
      eventBus.send(address, data, options);
    }
    return Completable.complete();
  }

  @Override
  public String name() {
    return name;
  }
}
