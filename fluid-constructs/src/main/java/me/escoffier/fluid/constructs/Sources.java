package me.escoffier.fluid.constructs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.spi.SourceFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Sources {

    private static Map<String, Source<?>> registered = new ConcurrentHashMap<>();

    public static synchronized <T> void register(Source<T> source) {
        registered.put(Objects.requireNonNull(source.name(), "the source has no name"), source);
    }

    public static synchronized <T> void register(String name, Source<T> source) {
        registered.put(Objects.requireNonNull(name, "the name must be provided"), source);
    }

    public static synchronized <T> void unregister(Source<T> source) {
        registered.remove(Objects.requireNonNull(source.name(), "the source has no name"));
    }

    public static synchronized <T> void unregister(String name) {
        registered.remove(Objects.requireNonNull(name, "the name must be provided"));
    }

    public static  <T> Source<T> get(String name) {
        return (Source<T>) registered.get(Objects.requireNonNull(name, "the name must be provided"));
    }

    public static synchronized void reset() {
        registered.clear();
    }

    public static void load(Vertx vertx) {
        Config load = ConfigFactory.load();

        try {
            Config sources = load.getConfig("sources");
            Collection<String> names = sources.root().keySet();

            for (String name : names) {
                Config config = sources.getConfig(name);
                Source<?> source = create(vertx, name, config);
                register(name, source);
            }
        } catch (ConfigException e) {
            // No sources
        }


    }

    private static Source<?> create(Vertx vertx, String name, Config config) {
        String type = config.getString("type");
        if (type == null) {
            throw new NullPointerException("Invalid configuration, the config " + name + " has no `type`");
        }

        SourceFactory factory = lookupForFactory(type);
        if (factory == null) {
            throw new NullPointerException("Invalid configuration, the sink type " + type + " is unknown");
        }

        String output = config.root()
            .render(ConfigRenderOptions.concise()
                .setJson(true).setComments(false).setFormatted(false));

        return factory.create(vertx, new JsonObject(output).put("name", name)).blockingGet();
    }

    private static SourceFactory lookupForFactory(String type) {
        ServiceLoader<SourceFactory> loader = ServiceLoader.load(SourceFactory.class);
        for (SourceFactory next : loader) {
            if (type.equalsIgnoreCase(next.name())) {
                return next;
            }
        }
        return null;
    }


    public static <T> Source<T> get(String name, Class<T> clazz) {
        return (Source<T>) get(name);
    }
}
