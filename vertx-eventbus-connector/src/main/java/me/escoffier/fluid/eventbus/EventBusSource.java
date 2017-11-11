package me.escoffier.fluid.eventbus;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.Message;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSource<T> extends DataStreamImpl<Void, T> implements Source<T> {
    private final String name;

    public EventBusSource(Vertx vertx, JsonObject json) {
        super(null, vertx.eventBus()
            .<T>consumer(json.getString("address"))
            .toFlowable()
            .map(Message::body));
        name = json.getString("name");
    }

    @Override
    public String name() {
        return name;
    }
}
