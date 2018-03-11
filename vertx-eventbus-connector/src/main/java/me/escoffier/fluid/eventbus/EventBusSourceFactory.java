package me.escoffier.fluid.eventbus;

import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.spi.SourceFactory;

import java.util.Optional;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSourceFactory implements SourceFactory {
    @Override
    public String name() {
        return "eventbus";
    }

    @Override
    public <T> Single<Source<T>> create(Vertx vertx, String name, Config config) {
        String address = config.getString("address")
          .orElse(name);

        if (address == null) {
          throw new IllegalArgumentException("Either address or name must be set");
        }

        return Single.just(new EventBusSource<>(vertx, name, address, config));

    }
}
