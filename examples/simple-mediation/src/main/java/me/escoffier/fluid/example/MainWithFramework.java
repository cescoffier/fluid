package me.escoffier.fluid.example;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.Fluid;
import me.escoffier.fluid.constructs.Sinks;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MainWithFramework {

    public static void main(String[] args) throws IOException {
        Main.init();
        
        Fluid fluid = new Fluid();

        // Deploy some sensors using the "code" deployment
        fluid.deploy(MainWithFramework::createSensor);
        fluid.deploy(MainWithFramework::createSensor);

        // Deploy a mediator
        fluid.deploy(Mediator.class);

        fluid.vertx().deployVerticle(WebVerticle.class.getName());
    }


    private static void createSensor(Fluid fluid) {
        String id = UUID.randomUUID().toString();
        Random random = new Random();

        fluid.from(Flowable.interval(1000, TimeUnit.MILLISECONDS).subscribeOn(Schedulers.computation()))
            .transform(l -> new JsonObject().put("uuid", id).put("data", random.nextInt(100)))
            .to(Sinks.get("sensor"));
    }
}
