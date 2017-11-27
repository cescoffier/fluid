package me.escoffier.fluid.view;

import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.Map;

public interface DocumentView {

    Completable save(String collection, String key, Map<String, Object> document);

    Single<Map<String, Object>> findById(String collection, String key);

    Single<Long> count(String collection);

    Observable<DocumentWithKey> findAll(String collection);

    Completable remove(String collection, String key);

}