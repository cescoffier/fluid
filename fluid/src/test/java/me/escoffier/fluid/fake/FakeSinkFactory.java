package me.escoffier.fluid.fake;

import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.impl.ListSink;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.spi.SinkFactory;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FakeSinkFactory implements SinkFactory {
  @Override
  public String name() {
    return "fake-sink";
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Single<Sink<T>> create(Vertx vertx, String name, Config conf) {
    return Single.just(new FakeSink(name));
  }

  private class FakeSink<T> extends ListSink<T> implements Sink<T> {
    private final String name;

    FakeSink(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }
  }
}
