package me.escoffier.fluid.view;

import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.Objects;

/**
 * Represent a document and its associated key.
 */
public class DocumentWithKey {

  /**
   * The key, must not be {@code null}
   */
  private final String key;

  /**
   * The content of the document.
   */
  private final Map<String, Object> document;

  /**
   * Creates a new instance of {@link DocumentWithKey}.
   *
   * @param key      the key, must not be {@code null}
   * @param document the document, must not be {@code null}
   */
  public DocumentWithKey(String key, Map<String, Object> document) {
    this.key = Objects.requireNonNull(key, "The `key` must not be `null`");
    this.document = Objects.requireNonNull(document, "The `document` must not be `null`");
  }

  public String key() {
    return key;
  }

  public Map<String, Object> document() {
    return document;
  }

  public JsonObject asJson() {
    return new JsonObject(document);
  }

}
