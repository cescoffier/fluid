package me.escoffier.fluid.eventbus;

import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.spi.SinkFactory;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSinkFactory implements SinkFactory {
    @Override
    public String name() {
        return "eventbus";
    }

    @Override
    public <T> Single<Sink<T>> create(Vertx vertx, JsonObject json) {
        return Single.just(new EventBusSink<>(vertx, json));

    }
}
