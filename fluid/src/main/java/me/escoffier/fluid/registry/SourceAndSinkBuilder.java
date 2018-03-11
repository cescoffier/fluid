package me.escoffier.fluid.registry;

import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.Config;
import me.escoffier.fluid.config.FluidConfig;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.spi.SinkFactory;
import me.escoffier.fluid.spi.SourceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Creates sources and sink from the configuration.
 */
public class SourceAndSinkBuilder {

  private static final Logger LOGGER = LogManager.getLogger(FluidRegistry.class);

  public static Map<String, Source> createSourcesFromConfiguration(Vertx vertx, FluidConfig config) {
    Map<String, Source> map = new HashMap<>();

    Optional<Config> sources = config.getConfig("sources");
    if (sources.isPresent()) {
      Iterator<String> names = sources.get().names();
      while (names.hasNext()) {
        String name = names.next();
        LOGGER.info("Creating source from configuration `" + name + "`");
        Optional<Config> conf = sources.get().getConfig(name);
        Source<?> source = buildSource(vertx, name,
          conf.orElseThrow(() -> new IllegalStateException("Illegal configuration for source `" + name + "`")));
        map.put(name, source);
      }
    } else {
      LOGGER.warn("No sources configured from the fluid configuration");
    }

    return map;
  }

  public static Map<String, Sink> createSinksFromConfiguration(Vertx vertx, FluidConfig config) {
    Map<String, Sink> map = new HashMap<>();

    Optional<Config> sinks = config.getConfig("sinks");
    if (sinks.isPresent()) {
      Iterator<String> names = sinks.get().names();
      while (names.hasNext()) {
        String name = names.next();
        LOGGER.info("Creating sink from configuration `" + name + "`");
        Optional<Config> conf = sinks.get().getConfig(name);
        Sink<?> sink = buildSink(vertx, name,
          conf.orElseThrow(() -> new IllegalStateException("Illegal configuration for source `" + name + "`")));
        map.put(name, sink);
      }
    } else {
      LOGGER.warn("No sinks configured from the fluid configuration");
    }
    return map;
  }

  private static Source buildSource(Vertx vertx, String name, Config config) {
    String type = config.getString("type")
      .orElseThrow(() ->
        new NullPointerException("Invalid configuration, the config " + name + " has no `type`")
      );

    SourceFactory factory = lookupForSourceFactory(type)
      .orElseThrow(() ->
        new NullPointerException("Invalid configuration, the source type " + type + " is unknown")
      );

    return factory.create(vertx, name, config).blockingGet();
  }

  private static Sink buildSink(Vertx vertx, String name, Config config) {
    String type = config.getString("type")
      .orElseThrow(() ->
        new NullPointerException("Invalid configuration, the config " + name + " has no `type`")
      );

    SinkFactory factory = lookupForSinkFactory(type)
      .orElseThrow(() ->
        new NullPointerException("Invalid configuration, the sink type " + type + " is unknown")
      );

    return factory.create(vertx, name, config).blockingGet();
  }

  private static Optional<SourceFactory> lookupForSourceFactory(String type) {
    ServiceLoader<SourceFactory> loader = ServiceLoader.load(SourceFactory.class);
    Stream<SourceFactory> stream = StreamSupport.stream(loader.spliterator(), false);
    return stream.filter(factory -> type.equalsIgnoreCase(factory.name())).findFirst();
  }

  private static Optional<SinkFactory> lookupForSinkFactory(String type) {
    ServiceLoader<SinkFactory> loader = ServiceLoader.load(SinkFactory.class);
    Stream<SinkFactory> stream = StreamSupport.stream(loader.spliterator(), false);
    return stream.filter(factory -> type.equalsIgnoreCase(factory.name())).findFirst();
  }

}
