package me.escoffier.fluid.eventbus;

import com.fasterxml.jackson.databind.node.NullNode;
import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.Source;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Check the behavior of the {@link EventBusSourceFactory} class.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSourceFactoryTest {

  private Vertx vertx;
  private EventBusSourceFactory factory;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    factory = new EventBusSourceFactory();
  }

  @After
  public void tearDown() {
    vertx.close();
  }


  @Test
  public void testName() {
    assertThat(factory.name()).isEqualTo("eventbus");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreationWithoutParameter() {
    factory.create(vertx, null, new Config(NullNode.getInstance()));
  }

  @Test
  public void testCreationWithAddress() throws IOException {
    Single<Source<Object>> single = factory.create(vertx, null, new Config(new JsonObject().put("address", "an-address")));
    Source<Object> sink = single.blockingGet();
    assertThat(sink).isInstanceOf(EventBusSource.class);
  }

}
