package me.escoffier.fluid.camel.sink;

import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.internal.operators.completable.CompletableFromObservable;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.models.Message;
import me.escoffier.fluid.models.Sink;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;

import java.util.concurrent.CompletableFuture;

public class CamelSink<T> implements Sink<T> {

    private final String endpoint;

    private final CamelContext camelContext;

    private final ProducerTemplate producerTemplate;

    public CamelSink(JsonObject config) {
        endpoint = config.getString("endpoint");
        camelContext = new DefaultCamelContext();
        try {
            camelContext.start();
            producerTemplate = camelContext.createProducerTemplate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override public Completable dispatch(Message<T> message) {
        CompletableFuture<Object> result = producerTemplate.asyncSendBody(endpoint, message.payload());
        return new CompletableFromObservable<>(toObservable(result));
    }

    public CamelContext camelContext() {
        return camelContext;
    }

    // Helpers

    private static <T> Observable<T> toObservable(CompletableFuture<T> future) {
        return Observable.create(subscriber ->
                future.whenComplete((result, error) -> {
                    if (error != null) {
                        subscriber.onError(error);
                    } else {
                        subscriber.onNext(result);
                        subscriber.onComplete();
                    }
                }));
    }

}
