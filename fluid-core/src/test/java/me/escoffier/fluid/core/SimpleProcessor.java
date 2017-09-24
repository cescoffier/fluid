package me.escoffier.fluid.core;

import io.reactivex.Flowable;
import io.vertx.core.json.JsonObject;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SimpleProcessor extends Processor {

    @Override
    public void process() {
        Flowable<JsonObject> stream = from("input")
            .cast(JsonObject.class)
            .map(json -> json.put("timestamp", System.currentTimeMillis()));
        
        dispatch(stream, "output");
    }
}
