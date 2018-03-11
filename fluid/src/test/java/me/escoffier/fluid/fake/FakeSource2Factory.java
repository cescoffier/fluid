package me.escoffier.fluid.fake;

import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.spi.SourceFactory;

public class FakeSource2Factory implements SourceFactory {
  @Override
  public String name() {
    return "fake-source-2";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Single<Source<Integer>> create(Vertx vertx, String name, Config config) {
    return Single.just(Source.fromPayloads(config.getIntList("items")).named(name));
  }
}
