package me.escoffier.fluid.view.inmemory;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import me.escoffier.fluid.view.DocumentView;
import me.escoffier.fluid.view.DocumentWithKey;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

/**
 * An in-memory implementation of document view.
 */
public class InMemoryDocumentView implements DocumentView {

  public static final String NULL_COLLECTION_MESSAGE = "The `collection` must not be `null`";
  public static final String NULL_KEY_MESSAGE = "The `key` must not be `null`";
  private final Map<String, Map<String, Map<String, Object>>> documents = new LinkedHashMap<>();

  @Override
  public synchronized Completable save(String collection, String key, Map<String, Object> document) {
    Objects.requireNonNull(collection, NULL_COLLECTION_MESSAGE);
    Objects.requireNonNull(key, NULL_KEY_MESSAGE);
    Objects.requireNonNull(collection, "The `document` must not be `null`");

    return Completable.fromAction(() -> {
      Map<String, Map<String, Object>> collectionData = documents.computeIfAbsent(collection, k -> new LinkedHashMap<>());
      collectionData.put(key, document);
    });
  }

  @Override
  public synchronized Single<Map<String, Object>> findById(String collection, String key) {
    Objects.requireNonNull(collection, NULL_COLLECTION_MESSAGE);
    Objects.requireNonNull(key, NULL_KEY_MESSAGE);

    return Single.fromCallable(() -> {
      Map<String, Map<String, Object>> collectionData = documents.computeIfAbsent(collection, k -> new LinkedHashMap<>());
      return collectionData.get(key);
    });
  }

  @Override
  public synchronized Single<Long> count(String collection) {
    Objects.requireNonNull(collection, NULL_COLLECTION_MESSAGE);

    return Single.fromCallable(() -> {
      Map<String, Map<String, Object>> collectionData = documents.computeIfAbsent(collection, key -> new LinkedHashMap<>());
      return (long) collectionData.size();
    });
  }

  @Override
  public synchronized Flowable<DocumentWithKey> findAll(String collection) {
    Objects.requireNonNull(collection, NULL_COLLECTION_MESSAGE);
    List<DocumentWithKey> documentsWithIds = documents.computeIfAbsent(collection, key -> new LinkedHashMap<>()).entrySet().stream().
      map(entry -> new DocumentWithKey(entry.getKey(), entry.getValue())).collect(toList());
    return Flowable.fromIterable(documentsWithIds);
  }

  @Override
  public synchronized Completable remove(String collection, String key) {
    Objects.requireNonNull(collection, NULL_COLLECTION_MESSAGE);
    Objects.requireNonNull(key, NULL_KEY_MESSAGE);

    return Completable.fromAction(() -> {
      Map<String, Map<String, Object>> collectionData = documents.computeIfAbsent(collection, k -> new LinkedHashMap<>());
      collectionData.remove(key);
    });
  }

}
