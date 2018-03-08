package me.escoffier.fluid.spi;

import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.models.Sink;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface SinkFactory {

    String name();

    <T> Single<Sink<T>> create(Vertx vertx, JsonObject json);

}
