package me.escoffier.fluid.registry;

import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Component storing the sources and sinks created from the configuration. It also provides a way to
 * register other sources and sinks.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FluidRegistry {


  private static final String NAME_NOT_PROVIDED_MESSAGE = "The source/sink has no name or " +
    "the given `name` is `null`";

  private static Map<String, Source> sources = new ConcurrentHashMap<>();
  private static Map<String, Sink> sinks = new ConcurrentHashMap<>();

  public static synchronized void initialize(Vertx vertx) {
    sinks.putAll(SourceAndSinkBuilder.createSinksFromConfiguration(vertx));
    sources.putAll(SourceAndSinkBuilder.createSourcesFromConfiguration(vertx));
  }

  public static void reset() {
    sources.clear();
    sinks.clear();
  }

  public static synchronized <T> void register(Source<T> source) {
    sources.put(Objects.requireNonNull(source.name(), NAME_NOT_PROVIDED_MESSAGE), source);
  }

  public static synchronized <T> void register(Sink<T> sink) {
    sinks.put(Objects.requireNonNull(sink.name(), NAME_NOT_PROVIDED_MESSAGE), sink);
  }

  public static synchronized <T> void register(String name, Source<T> source) {
    sources.put(Objects.requireNonNull(name, NAME_NOT_PROVIDED_MESSAGE), source);
  }

  public static synchronized <T> void register(String name, Sink<T> sink) {
    sinks.put(Objects.requireNonNull(name, NAME_NOT_PROVIDED_MESSAGE), sink);
  }

  public static synchronized <T> void unregisterSource(String name) {
    sources.remove(Objects.requireNonNull(name, NAME_NOT_PROVIDED_MESSAGE));
  }

  public static synchronized <T> void unregisterSink(String name) {
    sinks.remove(Objects.requireNonNull(name, NAME_NOT_PROVIDED_MESSAGE));
  }

  @SuppressWarnings("unchecked")
  public static <T> Source<T> source(String name) {
    return (Source<T>) sources.get(Objects.requireNonNull(name, NAME_NOT_PROVIDED_MESSAGE));
  }

  @SuppressWarnings("unchecked")
  public static <T> Sink<T> sink(String name) {
    return (Sink<T>) sinks.get(Objects.requireNonNull(name, NAME_NOT_PROVIDED_MESSAGE));
  }

  @SuppressWarnings({"unused", "unchecked"})
  public static <T> Source<T> source(String name, Class<T> clazz) {
    return (Source<T>) sources.get(Objects.requireNonNull(name, NAME_NOT_PROVIDED_MESSAGE));
  }
}
