package me.escoffier.fluid.constructs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.config.FluidConfig;
import me.escoffier.fluid.spi.SinkFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Sinks {

    private static Map<String, Sink<?>> registered = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LogManager.getLogger(Sinks.class);


    public static synchronized void reset() {
        registered.clear();
    }

    public static void load(Vertx vertx) {
        Optional<JsonNode> node = FluidConfig.get("sinks");

        if (node.isPresent()) {
            Iterator<String> names = node.get().fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                LOGGER.info("Creating sink from configuration `" + name + "`");
                JsonNode conf = node.get().get(name);
                Sink<?> source = create(FluidConfig.mapper(), vertx, name, conf);
                register(name, source);
            }
        } else {
            LOGGER.warn("No sinks configured from the fluid configuration");
        }
    }

    private static Sink<?> create(ObjectMapper mapper, Vertx vertx, String name, JsonNode config) {
        String type = config.get("type").asText(null);
        if (type == null) {
            throw new NullPointerException("Invalid configuration, the config " + name + " has no `type`");
        }

        SinkFactory factory = lookupForFactory(type);
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
