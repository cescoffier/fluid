package me.escoffier.fluid.constructs;

import io.reactivex.Completable;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class CacheSink<T> implements Sink<T> {

    List<T> buffer = new ArrayList<>();

    @Override
    public Completable dispatch(T data) {
        buffer.add(data);
        return Completable.complete();
    }

    public List<T> cache() {
        return buffer;
    }
}
