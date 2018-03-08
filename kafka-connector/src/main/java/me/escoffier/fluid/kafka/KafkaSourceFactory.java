package me.escoffier.fluid.kafka;

import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.spi.SourceFactory;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSourceFactory implements SourceFactory {
    @Override
    public String name() {
        return "kafka";
    }

    @Override
    public <T> Single<Source<T>> create(Vertx vertx, JsonObject json) {
        return Single.just(new KafkaSource(vertx, json));
    }
}
