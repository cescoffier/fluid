package me.escoffier.fluid.models;

import io.reactivex.Flowable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;

import static me.escoffier.fluid.models.CommonHeaders.GROUP_KEY;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class GroupedDataStream<K, T> implements Publisher<Message<T>> {

  private final K key;
  private final Publisher<Message<T>> items;

  public GroupedDataStream(K key, Publisher<Message<T>> items) {
    this.key = Objects.requireNonNull(key, "The `key` cannot be `null`");
    this.items = Flowable.fromPublisher(Objects.requireNonNull(items, "The `items` cannot be `null`"))
      .map(d -> d.with(GROUP_KEY, key));
  }

  @Override
  public void subscribe(Subscriber<? super Message<T>> s) {
    items.subscribe(s);
  }

  public K key() {
    return key;
  }
}
