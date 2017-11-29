package me.escoffier.fluid.constructs.impl;


import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.Data;
import me.escoffier.fluid.constructs.DataStream;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class StreamConnector<T> implements Processor<Data<T>, Data<T>> {

    private Flowable<Data<T>> source;
    private Subscriber<? super Data<T>> sub;

    public synchronized void connectDownstream(DataStream<T> src) {
        if (source != null) {
            throw new IllegalStateException("Connectable stream already connected");
        } else {
            source = src.flow();
        }
    }

    @Override
    public void subscribe(Subscriber<? super Data<T>> s) {
        synchronized (this) {
            if (source == null) {
                s.onError(new Exception("Connectable stream not connected"));
                return;
            }
        }
        sub = s;
        source.subscribe(this);
    }

    @Override
    public void onSubscribe(Subscription s) {
        sub.onSubscribe(s);
    }

    @Override
    public void onNext(Data<T> s) {
        sub.onNext(s);
    }

    @Override
    public void onError(Throwable t) {
        sub.onError(t);
    }

    @Override
    public void onComplete() {
        sub.onComplete();
    }
}
