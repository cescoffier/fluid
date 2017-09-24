package me.escoffier.fluid.core.sensor;

import hu.akarnokd.rxjava2.math.MathFlowable;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.reactivex.ext.web.client.WebClient;
import me.escoffier.fluid.core.Processor;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ProcessorB extends Processor {

    @Override
    public void process() {
        WebClient client = WebClient.create(vertx, new WebClientOptions()
            .setDefaultHost("localhost")
            .setDefaultPort(8085));

        Flowable<Double> average = from("average");

        average
            .filter(d -> d > 0)
            .map(val -> new JsonObject().put("average", val))
            .flatMapSingle(json -> client.post("/").rxSendJsonObject(json).map(x -> json))
            .flatMapSingle(json -> dispatch(json, "result"))
            .subscribe();
    }


}
