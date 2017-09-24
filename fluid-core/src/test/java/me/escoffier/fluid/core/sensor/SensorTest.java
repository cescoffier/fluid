package me.escoffier.fluid.core.sensor;

import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;
import io.vertx.reactivex.core.http.HttpServer;
import me.escoffier.fluid.core.FluidTestBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sensor-A - >
 * >> -- Processor A --> Processor B --> Result
 * Sensor-B - >
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SensorTest extends FluidTestBase {

    @Test
    public void test(TestContext tc) {
        System.setProperty("fluid-config", "config/sensor.yaml");

        Single<String> deploySensorA =
            vertx.rxDeployVerticle(Sensor.class.getName(), new DeploymentOptions().setConfig(getSensorConfig("A")));
        Single<String> deploySensorB =
            vertx.rxDeployVerticle(Sensor.class.getName(), new DeploymentOptions().setConfig(getSensorConfig("B")));

        Single<String> deployProcessorA = vertx.rxDeployVerticle(ProcessorA.class.getName());
        Single<String> deployProcessorB = vertx.rxDeployVerticle(ProcessorB.class.getName());

        Async async2 = tc.async();
        Single<HttpServer> startHttpServer = vertx.createHttpServer()
            .requestHandler(req -> req.bodyHandler(buffer -> {
                if (! async2.isCompleted()) {
                    async2.complete();
                }
            }))
            .rxListen(8085);

        deploySensorA
            .flatMap(x -> deploySensorB)
            .flatMap(x -> deployProcessorA)
            .flatMap(x -> deployProcessorB)
            .flatMap(x -> startHttpServer)
            .subscribe((res, err) -> {
                if (err != null) {
                    tc.fail(err);
                }
            });

        Async async1 = tc.async();
        kafkaCluster.useTo().consumeStrings("result", 4, 60, TimeUnit.SECONDS, async1::complete,
            (x, value) -> {
                JsonObject data = new JsonObject(value);
                assertThat(data.getLong("average")).isNotNull();
                return true;
            });
    }

    private JsonObject getSensorConfig(String name) {
        Map config = kafkaCluster.useTo().getProducerProperties(name);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonObjectSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonObjectSerializer.class.getName());
        config.put("name", name);
        return new JsonObject(config);
    }
}
