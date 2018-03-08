package me.escoffier.fluid.eventbus;

import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.spi.SourceFactory;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSourceFactory implements SourceFactory {
    @Override
    public String name() {
        return "eventbus";
    }

    @Override
    public <T> Single<Source<T>> create(Vertx vertx, JsonObject json) {
        String address = json.getString("address");
        if (address == null) {
            String name = json.getString("name");
            if (name != null) {
                json.put("address", name);
            } else {
                throw new IllegalArgumentException("Either address or name must be set");
            }
        }
        return Single.just(new EventBusSource<>(vertx, json));

    }
}
