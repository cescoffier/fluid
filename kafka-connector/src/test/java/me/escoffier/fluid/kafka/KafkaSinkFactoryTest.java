package me.escoffier.fluid.kafka;

import com.fasterxml.jackson.databind.node.NullNode;
import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.models.Sink;
import org.apache.kafka.common.config.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the behavior of the {@link KafkaSinkFactory}.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class KafkaSinkFactoryTest {

  private Vertx vertx;
  private KafkaSinkFactory factory;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    factory = new KafkaSinkFactory();
  }

  @After
  public void tearDown() {
    vertx.close();
  }


  @Test
  public void testName() {
    assertThat(factory.name()).isEqualTo("kafka");
  }

  @Test(expected = ConfigException.class)
  public void testCreationWithoutParameter() {
    factory.create(vertx, null, new Config(NullNode.getInstance()));
  }

  @Test
  public void testCreationWithMinimalConfiguration() throws IOException {
    Single<Sink<Object>> single = factory.create(vertx, null,
      new Config(new JsonObject()
        .put("bootstrap.servers", "localhost:9092")
        .put("key.serializer", JsonObjectSerializer.class.getName())
        .put("value.serializer", JsonObjectSerializer.class.getName())));
    Sink<Object> sink = single.blockingGet();
    assertThat(sink).isInstanceOf(KafkaSink.class);
  }

}
