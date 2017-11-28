package me.escoffier.fluid.fake;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.constructs.DataStream;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.constructs.impl.DataStreamImpl;
import me.escoffier.fluid.spi.SourceFactory;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FakeSourceFactory implements SourceFactory {
  @Override
  public String name() {
    return "fake-source";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Single<Source<String>> create(Vertx vertx, JsonObject json) {
    return Single.just(new FakeSourceImpl(json));
  }

  private class FakeSourceImpl extends DataStreamImpl<Void, String>
    implements Source<String> {

    private String name;

    public FakeSourceImpl(JsonObject json) {
      super(null, Flowable.fromArray("a", "b", "c"));
      name = json.getString("name");
    }

    @Override
    public String name() {
      return name;
    }
  }
}
