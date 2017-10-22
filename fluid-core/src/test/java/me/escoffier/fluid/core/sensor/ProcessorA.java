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

        /*
        
            join(sensor-a, sensor-b)
                .....

                from(sensor-a)
                    .mergeWith(sensorB)
                    .transformWith(...)
                    .to("average")

            from("sensor-a")
            .transformWith(instance)
                .transformWith(x -> {   Function<Flowable, Flowable> wrapped into a "FluidFlow"
                   return x
                        .mergeWith(from("B))
                        .map(json -> json.getDouble("value"))
                        .window(10)
                        .flatMap(MathFlowable::averageDouble);
                 })
                .transformWith(x -> { .... })
            .to("result")


            public class InstanceClass {
                @Processor
                public void process(@Input("sensor-a") input1, @Input("sensor-b") input2, @Output("average") output) {
                    //....
                }
            }
         */


        Flowable<JsonObject> measuresFromA = from("sensor-A");
        Flowable<JsonObject> measuresFromB = from("sensor-B");

        Flowable<Double> result = measuresFromA
            .mergeWith(measuresFromB)
            .map(json -> json.getDouble("value"))
            .window(10)
            .flatMap(MathFlowable::averageDouble);

        dispatch(result, "average");
    }
}
