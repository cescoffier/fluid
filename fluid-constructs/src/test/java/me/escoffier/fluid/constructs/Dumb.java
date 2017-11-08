package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import io.reactivex.schedulers.Schedulers;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Dumb {


    @Test
    void foo() {
        Flowable<String> flowable = Flowable.fromArray("1", "2", "3")
            .doOnSubscribe(s -> {
                System.out.println("A");
            })
            .map(s -> {
                return ">> " + s;
            })
            .doOnSubscribe(s -> {
                System.out.println("B");
            });
        
        Processor<String, String> proc = new Processor<String, String>() {
            Subscriber<? super String> sub;

            @Override
            public void subscribe(Subscriber<? super String> s) {
                sub = s;
                flowable.subscribe(this);
            }

            @Override
            public void onSubscribe(Subscription s) {
                sub.onSubscribe(s);
            }

            @Override
            public void onNext(String s) {
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
        };


        Flowable.fromPublisher(proc)
            .doOnNext(System.out::println)
            .blockingSubscribe();

    }

    @Test
    void bar() throws InterruptedException {
        Flowable<String> f1 = Flowable.fromArray("a", "b", "c")
            .delay(10, TimeUnit.MILLISECONDS);
        Flowable<String> f2 = Flowable.fromArray("d", "e", "f");

        Flowable.merge(f1, f2)
            .subscribe(s -> System.out.println(">> " + s));

        Thread.sleep(1000);
    }


}
