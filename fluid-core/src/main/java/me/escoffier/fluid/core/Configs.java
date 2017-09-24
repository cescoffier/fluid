package me.escoffier.fluid.core;

import io.vertx.core.json.JsonObject;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Configs {

    public static JsonObject findSinkConfig(JsonObject json, String name) {
        if (json == null) {
            throw new RuntimeException("No `sinks` in the configuration");
        }
        JsonObject object = json.getJsonObject(name);
        if (object == null) {
            throw new RuntimeException("Unknown sink `" + name + "` in the configuration, known sinks " +
                "are: " + json.fieldNames());
        }
        return object;
    }

    public static JsonObject findSourceConfig(JsonObject json, String name) {
        if (json == null) {
            throw new RuntimeException("No `sources` in the configuration");
        }
        JsonObject object = json.getJsonObject(name);
        if (object == null) {
            throw new RuntimeException("Unknown source `" + name + "` in the configuration, known sources " +
                "are: " + json.fieldNames());
        }
        return object;
    }
}
