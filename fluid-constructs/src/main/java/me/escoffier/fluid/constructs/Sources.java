package me.escoffier.fluid.constructs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.FluidConfig;
import me.escoffier.fluid.spi.SourceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Sources {

  private static final String NAME_NOT_PROVIDED_MESSAGE = "The source has no name or the given `name` is `null`";


  private static final Logger LOGGER = LogManager.getLogger(Sources.class);
    private static Map<String, Source<?>> registered = new ConcurrentHashMap<>();

    public static synchronized <T> void register(Source<T> source) {
        registered.put(Objects.requireNonNull(source.name(), NAME_NOT_PROVIDED_MESSAGE), source);
    }

    public static synchronized <T> void register(String name, Source<T> source) {
        registered.put(Objects.requireNonNull(name, NAME_NOT_PROVIDED_MESSAGE), source);
    }

    public static synchronized <T> void unregister(Source<T> source) {
        registered.remove(Objects.requireNonNull(source.name(), NAME_NOT_PROVIDED_MESSAGE));
    }

    public static synchronized <T> void unregister(String name) {
        registered.remove(Objects.requireNonNull(name, NAME_NOT_PROVIDED_MESSAGE));
    }

    public static <T> Source<T> get(String name) {
        return (Source<T>) registered.get(Objects.requireNonNull(name, NAME_NOT_PROVIDED_MESSAGE));
    }

    public static synchronized void reset() {
        registered.clear();
    }

    public static void load(Vertx vertx) {
        Optional<JsonNode> node = FluidConfig.get("sources");

        if (node.isPresent()) {
            Iterator<String> names = node.get().fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                LOGGER.info("Creating source from configuration `" + name + "`");
                JsonNode conf = node.get().get(name);
                Source<?> source = create(FluidConfig.mapper(), vertx, name, conf);
                register(name, source);
            }
        } else {
            LOGGER.warn("No sources configured from the fluid configuration");
        }
    }

    private static Source<?> create(ObjectMapper mapper, Vertx vertx, String name, JsonNode config) {
        String type = config.get("type").asText(null);
        if (type == null) {
            throw new NullPointerException("Invalid configuration, the config " + name + " has no `type`");
        }

        SourceFactory factory = lookupForFactory(type);
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
