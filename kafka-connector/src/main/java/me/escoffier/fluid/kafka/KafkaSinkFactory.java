package me.escoffier.fluid.kafka;

import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.spi.SinkFactory;
import me.escoffier.fluid.spi.SourceFactory;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSinkFactory implements SinkFactory {
    @Override
    public String name() {
        return "kafka";
    }

    @Override
    public <T> Single<Sink<T>> create(Vertx vertx, JsonObject json) {
        return Single.just(new KafkaSink<>(vertx, json));
    }
}
