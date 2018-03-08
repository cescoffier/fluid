package me.escoffier.fluid.fake;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.models.AbstractSource;
import me.escoffier.fluid.models.Data;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.spi.SourceFactory;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FakeSource2Factory implements SourceFactory {
  @Override
  public String name() {
    return "fake-source-2";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Single<Source<Integer>> create(Vertx vertx, JsonObject json) {
    return Single.just(new FakeSourceImpl(json));
  }

  private class FakeSourceImpl extends AbstractSource<Integer> {

    private String name;

    @SuppressWarnings("unchecked")
    public FakeSourceImpl(JsonObject json) {
      super(Flowable.fromIterable(
        json.getJsonArray("items").getList()).map(Data::new)
      , null, null);
      name = json.getString("name");
    }

    @Override
    public String name() {
      return name;
    }
  }
}
