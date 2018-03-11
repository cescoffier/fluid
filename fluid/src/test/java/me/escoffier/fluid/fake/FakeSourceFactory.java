package me.escoffier.fluid.fake;

import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.Source;
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
  public Single<Source<String>> create(Vertx vertx, String name, Config config) {
    return Single.just(Source.from("a", "b", "c").named(name));
  }
}
