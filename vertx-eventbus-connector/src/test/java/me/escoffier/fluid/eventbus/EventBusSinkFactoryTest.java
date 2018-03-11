package me.escoffier.fluid.eventbus;

import com.fasterxml.jackson.databind.node.NullNode;
import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.Sink;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Check the behavior of the {@link EventBusSinkFactory} class.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class EventBusSinkFactoryTest {

  private Vertx vertx;
  private EventBusSinkFactory factory;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    factory = new EventBusSinkFactory();
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
    Single<Sink<Object>> single = factory.create(vertx,
      null,
      new Config(new JsonObject().put("address", "an-address")));
    Sink<Object> sink = single.blockingGet();
    assertThat(sink).isInstanceOf(EventBusSink.class);
  }

}
