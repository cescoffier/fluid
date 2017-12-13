package me.escoffier.fluid.view.inmemory;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import me.escoffier.fluid.view.DocumentView;
import me.escoffier.fluid.view.DocumentWithKey;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

@RunWith(VertxUnitRunner.class)
public class InMemoryDocumentViewTest {

  private DocumentView view = new InMemoryDocumentView();

  private String collection = UUID.randomUUID().toString();

  private String key = UUID.randomUUID().toString();

  private Map<String, Object> document = new LinkedHashMap<>();

  {
    document.put("foo", "bar");
  }

  @Test
  public void shouldSave(TestContext context) {
    Async async = context.async();
    view.save(collection, key, document).subscribe(() ->
      view.findById(collection, key).subscribe(document -> {
        assertThat(document).isEqualTo(this.document);
        async.complete();
      })
    );
  }

  @Test
  public void shouldCallSubscriberOnSave(TestContext context) {
    Async async = context.async();
    view.save(collection, key, document).subscribe(async::complete);
  }

  @Test
  public void shouldCount(TestContext context) {
    Async async = context.async();
    view.save(collection, key, document).subscribe(() ->
      view.findById(collection, key).subscribe(document ->
        view.count(collection).subscribe(count -> {
          assertThat(count).isEqualTo(1);
          async.complete();
        })
      )
    );
  }

  @Test
  public void shouldFindAll() {
    view.save(collection, key, document).subscribe(() -> {
      List<DocumentWithKey> documents = new LinkedList<>();
      view.findById(collection, key).subscribe(document ->
        view.findAll(collection).subscribe(documents::add)
      );
      assertThat(documents).hasSize(1);
      assertThat(documents.get(0).key()).isEqualTo(key);
      assertThat(documents.get(0).document()).isEqualTo(document);
      assertThat(documents.get(0).asJson()).contains(entry("foo", "bar"));
    });
  }

  @Test
  public void shouldRemove(TestContext context) {
    Async async = context.async();
    view.save(collection, key, document).subscribe(() ->
      view.remove(collection, key).subscribe(() ->
        view.count(collection).subscribe(count -> {
          assertThat(count).isEqualTo(0);
          async.complete();
        })
      )
    );
  }

}
