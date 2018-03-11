package me.escoffier.fluid.spi;

import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.config.FluidConfig;
import me.escoffier.fluid.models.Source;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface SourceFactory {

    String name();

    <T> Single<Source<T>> create(Vertx vertx, String nameIfAny, Config config);

}
