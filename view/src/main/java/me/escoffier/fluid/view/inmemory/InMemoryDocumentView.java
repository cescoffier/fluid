package me.escoffier.fluid.view.inmemory;

import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import me.escoffier.fluid.view.DocumentView;
import me.escoffier.fluid.view.DocumentWithKey;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.reactivex.Completable.complete;
import static io.reactivex.Observable.fromIterable;
import static io.reactivex.Single.just;
import static java.util.stream.Collectors.toList;

public class InMemoryDocumentView implements DocumentView {

    private final Map<String, Map<String, Map<String, Object>>> documents = new LinkedHashMap<>();

    synchronized @Override public Completable save(String collection, String key, Map<String, Object> document) {
        Map<String, Map<String, Object>> collectionData = documents.computeIfAbsent(collection, k -> new LinkedHashMap<>());
        collectionData.put(key, document);
        return complete();
    }

    synchronized @Override public Single<Map<String, Object>> findById(String collection, String key) {
        Map<String, Map<String, Object>> collectionData = documents.computeIfAbsent(collection, k -> new LinkedHashMap<>());
        return just(collectionData.get(key));
    }

    synchronized @Override public Single<Long> count(String collection) {
        Map<String, Map<String, Object>> collectionData = documents.computeIfAbsent(collection, key -> new LinkedHashMap<>());
        return just((long) collectionData.size());
    }

    synchronized @Override public Observable<DocumentWithKey> findAll(String collection) {
        List<DocumentWithKey> documentsWithIds = documents.computeIfAbsent(collection, key -> new LinkedHashMap<>()).entrySet().stream().
                map(entry -> new DocumentWithKey(entry.getKey(), entry.getValue())).collect(toList());
        return fromIterable(documentsWithIds);
    }

    synchronized @Override public Completable remove(String collection, String key) {
        Map<String, Map<String, Object>> collectionData = documents.computeIfAbsent(collection, k -> new LinkedHashMap<>());
        collectionData.remove(key);
        return complete();
    }

}