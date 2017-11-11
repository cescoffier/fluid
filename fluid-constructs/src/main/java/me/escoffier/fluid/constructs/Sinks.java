package me.escoffier.fluid.constructs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.spi.SinkFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Sinks {

    private static Map<String, Sink<?>> registered = new ConcurrentHashMap<>();

    public static synchronized void reset() {
        registered.clear();
    }

    public static void load(Vertx vertx) {
        Config load = ConfigFactory.load();

        Config sinks = load.getConfig("sinks");
        Set<String> names = sinks.root().keySet();

        for (String name : names) {
            Config config = sinks.getConfig(name);
            Sink<?> sink = create(vertx, name, config);
            register(name, sink);
        }
    }

    private static Sink<?> create(Vertx vertx, String name, Config config) {
        String type = config.getString("type");
        if (type == null) {
            throw new NullPointerException("Invalid configuration, the config " + name + " has no `type`");
        }

        SinkFactory factory = lookupForFactory(type);
        if (factory == null) {
            throw new NullPointerException("Invalid configuration, the sink type " + type + " is unknown");
        }

        String output = config.root()
            .render(ConfigRenderOptions.concise()
            .setJson(true).setComments(false).setFormatted(false));

        return factory.create(vertx, new JsonObject(output).put("name", name)).blockingGet();
    }

    private static SinkFactory lookupForFactory(String type) {
        ServiceLoader<SinkFactory> loader = ServiceLoader.load(SinkFactory.class);
        for (SinkFactory next : loader) {
            if (type.equalsIgnoreCase(next.name())) {
                return next;
            }
        }
        return null;
    }

    public static synchronized <T> void register(Sink<T> sink) {
        registered.put(Objects.requireNonNull(sink.name(), "the sink has no name"), sink);
    }

    public static synchronized <T> void register(String name, Sink<T> sink) {
        registered.put(Objects.requireNonNull(name, "the name must be provided"), sink);
    }

    public static synchronized <T> void unregister(Sink<T> sink) {
        registered.remove(Objects.requireNonNull(sink.name(), "the sink has no name"));
    }

    public static synchronized <T> void unregister(String name) {
        registered.remove(Objects.requireNonNull(name, "the name must be provided"));
    }

    public static <T> Sink<T> get(String name) {
        return (Sink<T>) registered.get(Objects.requireNonNull(name, "the name must be provided"));
    }




}
