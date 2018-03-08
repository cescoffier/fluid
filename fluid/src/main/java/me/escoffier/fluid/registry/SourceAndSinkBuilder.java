package me.escoffier.fluid.registry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.FluidConfig;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.spi.SinkFactory;
import me.escoffier.fluid.spi.SourceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Creates sources and sink from the configuration.
 */
public class SourceAndSinkBuilder {

  private static final Logger LOGGER = LogManager.getLogger(FluidRegistry.class);

  public static Map<String, Source> createSourcesFromConfiguration(Vertx vertx) {
    Map<String, Source> map = new HashMap<>();
    Optional<JsonNode> node = FluidConfig.get("sources");

    if (node.isPresent()) {
      Iterator<String> names = node.get().fieldNames();
      while (names.hasNext()) {
        String name = names.next();
        LOGGER.info("Creating source from configuration `" + name + "`");
        JsonNode conf = node.get().get(name);
        Source<?> source = buildSource(FluidConfig.mapper(), vertx, name, conf);
        map.put(name, source);
      }
    } else {
      LOGGER.warn("No sources configured from the fluid configuration");
    }

    return map;
  }

  public static Map<String, Sink> createSinksFromConfiguration(Vertx vertx) {
    Map<String, Sink> map = new HashMap<>();
    Optional<JsonNode> node = FluidConfig.get("sinks");

    if (node.isPresent()) {
      Iterator<String> names = node.get().fieldNames();
      while (names.hasNext()) {
        String name = names.next();
        LOGGER.info("Creating sink from configuration `" + name + "`");
        JsonNode conf = node.get().get(name);
        Sink<?> source = buildSink(FluidConfig.mapper(), vertx, name, conf);
        map.put(name, source);
      }
    } else {
      LOGGER.warn("No sinks configured from the fluid configuration");
    }
    return map;
  }

  private static Source buildSource(ObjectMapper mapper, Vertx vertx, String name, JsonNode config) {
    String type = config.get("type").asText(null);
    if (type == null) {
      throw new NullPointerException("Invalid configuration, the config " + name + " has no `type`");
    }

    SourceFactory factory = lookupForSourceFactory(type);
    if (factory == null) {
      throw new NullPointerException("Invalid configuration, the source type " + type + " is unknown");
    }

    try {
      String json = mapper.writeValueAsString(config);
      return factory.create(vertx, new JsonObject(json).put("name", name)).blockingGet();
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid configuration for " + name, e);
    }
  }

  private static Sink buildSink(ObjectMapper mapper, Vertx vertx, String name, JsonNode config) {
    String type = config.get("type").asText(null);
    if (type == null) {
      throw new NullPointerException("Invalid configuration, the config " + name + " has no `type`");
    }

    SinkFactory factory = lookupForSinkFactory(type);
    if (factory == null) {
      throw new NullPointerException("Invalid configuration, the sink type " + type + " is unknown");
    }

    try {
      String json = mapper.writeValueAsString(config);
      return factory.create(vertx, new JsonObject(json).put("name", name)).blockingGet();
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid configuration for " + name, e);
    }
  }

  private static SourceFactory lookupForSourceFactory(String type) {
    ServiceLoader<SourceFactory> loader = ServiceLoader.load(SourceFactory.class);
    for (SourceFactory next : loader) {
      if (type.equalsIgnoreCase(next.name())) {
        return next;
      }
    }
    return null;
  }

  private static SinkFactory lookupForSinkFactory(String type) {
    ServiceLoader<SinkFactory> loader = ServiceLoader.load(SinkFactory.class);
    for (SinkFactory next : loader) {
      if (type.equalsIgnoreCase(next.name())) {
        return next;
      }
    }
    return null;
  }

}
