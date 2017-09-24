package me.escoffier.fluid.core.sensor;

import hu.akarnokd.rxjava2.math.MathFlowable;
import io.reactivex.Flowable;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.core.Processor;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ProcessorA extends Processor {

    @Override
    public void process() {
        Flowable<JsonObject> measuresFromA = from("sensor-A");
        Flowable<JsonObject> measuresFromB =from("sensor-B");

        Flowable<Double> result = measuresFromA
            .mergeWith(measuresFromB)
            .map(json -> json.getDouble("value"))
            .window(10)
            .flatMap(MathFlowable::averageDouble);

        dispatch(result, "average");
    }
}
