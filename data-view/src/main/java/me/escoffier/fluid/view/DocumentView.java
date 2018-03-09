package me.escoffier.fluid.view;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import me.escoffier.fluid.models.Message;

import java.util.Map;

/**
 * Interface used to store, retrieve and delete a {@code document} representing a extraction of information from the
 * received {@link Message}.
 * <p>
 * The documents are organized in @{code collections} and are stored to a specific {@code key}. A {@code document} is
 * just a @{code Map&lt;String, Object&gt;} containing the data.
 * <p>
 * When a document is modified, it must be explicitly re-saved.
 */
public interface DocumentView {

  /**
   * Saves the given view into the current store.
   *
   * @param collection the name of the collection
   * @param key        the document key
   * @param document   the content
   * @return a {@link Completable} indicating when the @{code save} operation has completed.
   */
  Completable save(String collection, String key, Map<String, Object> document);

  /**
   * Finds a document if exist or returns an empty one.
   *
   * @param collection the name of the collection
   * @param key        the document key
   * @return a {@link Single} providing the retrieved {@code document}. Note that if the document has not been found, an
   * empty one is returned
   */
  Single<Map<String, Object>> findById(String collection, String key);

  /**
   * Retrieves the number of document stored in the collection.
   *
   * @param collection the name of the collection
   * @return a {@link Single} providing the number of document stored in the given collection. {@code 0} if the collection
   * is unknown.
   */
  Single<Long> count(String collection);

  /**
   * Retrieves all the documents stored in the collection.
   *
   * @param collection the name of the collection
   * @return a {@link Flowable} providing the document. Empty if the collection is unknown.
   */
  Flowable<DocumentWithKey> findAll(String collection);

  /**
   * Removes a specific document in a collection
   *
   * @param collection the name of the collection
   * @param key        the key of the document
   * @return a {@link Completable} indicating when the removal has completed.
   */
  Completable remove(String collection, String key);

}
