package me.escoffier.fluid.camel.sink;

import com.fasterxml.jackson.databind.node.NullNode;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import io.vertx.core.json.JsonObject;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.Sink;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the behavior of the {@link CamelSinkFactory}.
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class CamelSinkFactoryTest {

  private Vertx vertx;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() {
    vertx.close();
  }


  @Test
  public void testName() {
    CamelSinkFactory factory = new CamelSinkFactory();
    assertThat(factory.name()).isEqualTo("camel");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreationWithoutParameter() {
    CamelSinkFactory factory = new CamelSinkFactory();
    factory.create(vertx, null , new Config(NullNode.getInstance()));
  }

  @Test
  public void testCreationWithEndpoint() throws IOException {
    CamelSinkFactory factory = new CamelSinkFactory();
    Single<Sink<Object>> single = factory.create(vertx, null ,
      new Config(new JsonObject().put("endpoint", "my-endpoint")));
    Sink<Object> sink = single.blockingGet();
    assertThat(sink).isInstanceOf(CamelSink.class);
  }



}
