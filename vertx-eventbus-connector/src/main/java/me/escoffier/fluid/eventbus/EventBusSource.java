package me.escoffier.fluid.eventbus;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.Message;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSource<T> extends DataStreamImpl<Void, T> implements Source<T> {
  private final String name;

  public EventBusSource(Vertx vertx, JsonObject json) {
    super(null, vertx.eventBus()
      .<T>consumer(json.getString("address"))
      .toFlowable()
      .map(EventBusSource::createData)
      .compose(upstream -> {
        Integer size = json.getInteger("multicast.buffer.size", -1);
        if (size != -1) {
          return upstream.replay(size).autoConnect();
        }

        Integer seconds = json.getInteger("multicast.buffer.period.ms", -1);
        if (seconds != -1) {
          return upstream.replay(seconds, TimeUnit.MILLISECONDS).autoConnect();
        }

        return upstream;
      }));
    name = json.getString("name");
  }

  private static <T> Data<T> createData(Message<T> msg) {
    Map<String, Object> map = new HashMap<>();
    msg.headers().names().forEach(s -> map.put(s, msg.headers().get(s)));
    return new Data<>(msg.body(), map);
  }

  @Override
  public String name() {
    return name;
  }
}
