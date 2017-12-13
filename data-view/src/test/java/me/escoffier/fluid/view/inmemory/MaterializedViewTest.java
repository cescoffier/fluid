package me.escoffier.fluid.view.inmemory;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.view.DocumentView;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Maps.newHashMap;

@RunWith(VertxUnitRunner.class)
public class MaterializedViewTest {

  private DocumentView materializedView = new InMemoryDocumentView();

  @Test
  public void shouldGenerateMaterializedView(TestContext context) {
    Async async = context.async();
    // Create data set
    Source.from(1, 2, 3).flow().
      // Generate materialized view
        flatMap(it -> done -> materializedView.save("numbers", it + "", newHashMap("number", it)).subscribe(done::onComplete)).
      subscribe();
    // Verify that view has been generated
    materializedView.count("numbers").subscribe(count -> {
      assertThat(count).isEqualTo(3);
      async.complete();
    });
  }

}
