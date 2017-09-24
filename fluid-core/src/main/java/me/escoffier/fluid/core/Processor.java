package me.escoffier.fluid.core;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.reactivex.CompletableHelper;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.reactivex.servicediscovery.ServiceDiscovery;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Processor extends AbstractVerticle {

    protected ServiceDiscovery discovery;
    private ConfigRetriever retriever;
    private JsonObject configuration;

    private Map<String, KafkaConsumer> consumers = new ConcurrentHashMap<>();
    private Map<String, KafkaWriteStream> producers = new ConcurrentHashMap<>();

    public synchronized <T> Flowable<T> from(String id) {
        Objects.requireNonNull(id, "source `id` cannot be null");

        JsonObject sources = configuration.getJsonObject("sources");

        JsonObject config;
        try {
             config = Configs.findSourceConfig(sources, id);
        } catch (Exception e) {
            return Flowable.error(e);
        }

        String topic = config.getString("topic");
        if (topic == null) {
            return Flowable.error(new Exception("Unknown topic in source `" + id + "` in the configuration"));
        }

        KafkaConsumer<String, T> consumer = getKafkaConsumerByName(id, config);
        return consumer.subscribe(topic).toFlowable()
            .doOnError(t -> t.printStackTrace()) // TODO error management
            .map(KafkaConsumerRecord::value);
    }

    private <T> KafkaConsumer<String, T> getKafkaConsumerByName(String id, JsonObject config) {
        KafkaConsumer<String, T> consumer = consumers.get(id);
        if (consumer == null) {
            consumer = KafkaConsumer.create(vertx, toMap(config));
            consumers.put(id, consumer);
        }
        return consumer;
    }

    private static Map<String, String> toMap(JsonObject jsonObject) {
        Map<String, String> map = new HashMap<>();
        jsonObject.getMap().forEach((key, value) -> map.put(key, value.toString()));
        return map;
    }

    public synchronized <T> Processor dispatch(Flowable<T> stream, String id) {
        Objects.requireNonNull(id, "sink `id` cannot be null");
        JsonObject sinks = configuration.getJsonObject("sinks");
        JsonObject config = Configs.findSinkConfig(sinks, id);
        String topic = config.getString("topic");
        if (topic == null) {
            throw new RuntimeException("Unknown topic in sink `" + id + "` in the configuration");
        }

        KafkaWriteStream<String, T> producer = getKafkaWriteStreamByName(id, config.getMap());
        stream
            .flatMapSingle(data -> Single.just(producer.write(new ProducerRecord<>(topic, data))))
            .doOnError(Throwable::printStackTrace)
            .subscribe();

        return this;
    }



    public synchronized <T> Single<T> dispatch(T value, String id) {
        Objects.requireNonNull(id, "sink `id` cannot be null");

        JsonObject sinks = configuration.getJsonObject("sinks");
        JsonObject config = Configs.findSinkConfig(sinks, id);
        String topic = config.getString("topic");
        if (topic == null) {
            throw new RuntimeException("Unknown topic in sink `" + id + "` in the configuration");
        }

        KafkaWriteStream<String, T> producer = getKafkaWriteStreamByName(id, config.getMap());

        producer.write(new ProducerRecord<>(topic, value));

        return Single.just(value);
    }

    private <T> KafkaWriteStream<String, T> getKafkaWriteStreamByName(String id, Map<String, Object> map) {
        KafkaWriteStream<String, T> producer = producers.get(id);
        if (producer == null) {
            producer = KafkaWriteStream.create(vertx.getDelegate(), map);
            producers.put(id, producer);
        }
        return producer;
    }

    @Override
    public void start(Future<Void> future) throws Exception {
        retriever = ConfigRetriever.create(vertx, new ConfigRetrieverOptions().setStores(getConfigurationStores()));

        // Retrieve config.
        retriever.rxGetConfig()
            .doOnSuccess(json -> this.configuration = json)

            // Initialize service discovery.
//            .flatMapCompletable(json -> {
//                Future<Void> fut = Future.future();
//                ServiceDiscovery.create(vertx, discovery -> {
//                    this.discovery = discovery;
//                    fut.complete();
//                });
//                return RxTools.from(fut).toCompletable();
//            })

            // Call "process"
            .toCompletable()
            .doOnComplete(this::process)

            .subscribe(CompletableHelper.toObserver(future));
    }

    public void process() {

    }

    private List<ConfigStoreOptions> getConfigurationStores() {
        return Collections.singletonList(
            new ConfigStoreOptions()
                .setType("file")
                .setFormat("yaml")
                .setConfig(new JsonObject().put("path",
                    Systems.getSystemPropertyOrEnvVar("fluid-config", "config/fluid-config.yaml")))
        );
    }

    @Override
    public void stop(Future<Void> future) throws Exception {
        retriever.close();
        producers.values().forEach(KafkaWriteStream::close);
        Observable<Object> close1 = Observable.fromIterable(consumers.values()).flatMap(cons -> cons.rxClose().toObservable());
        close1.subscribe(
            x -> {
            },
            future::fail,
            future::complete
        );
    }
}
