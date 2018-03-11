package me.escoffier.fluid.eventbus;

import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.CommonHeaders;
import me.escoffier.fluid.models.DefaultSource;
import me.escoffier.fluid.models.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSource<T> extends DefaultSource<T> {

  public EventBusSource(Vertx vertx, String name, String address, Config config) {
    super(vertx.eventBus()
      .<T>consumer(address)
      .toFlowable()
      .map(EventBusSource::createData)
      .compose(upstream -> {
        int size = config.getInt("multicast.buffer.size", -1);
        if (size != -1) {
          return upstream.replay(size).autoConnect();
        }

        int seconds = config.getInt("multicast.buffer.period.ms", -1);
        if (seconds != -1) {
          return upstream.replay(seconds, TimeUnit.MILLISECONDS).autoConnect();
        }

        return upstream;
      }), name, null);
  }

  private static <T> Message<T> createData(io.vertx.reactivex.core.eventbus.Message<T> msg) {
    Map<String, Object> map = new HashMap<>();
    msg.headers().names().forEach(s -> map.put(s, msg.headers().get(s)));
    map.put(CommonHeaders.ORIGINAL, msg);
    return new Message<>(msg.body(), map);
  }
}
