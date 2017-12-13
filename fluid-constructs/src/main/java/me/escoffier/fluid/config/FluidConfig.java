package me.escoffier.fluid.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;
import java.util.Optional;

/**
 * Utilities to read the Fluid configuration.
 * <p>
 * The configuration is read from the following location:
 * * `fluid-config` system property
 * * `FLUID_CONFIG` environment variable
 * * `fluid.yml` in the classpath
 * * `fluid.yaml` in the classpath
 */
public class FluidConfig {

    private static final Logger logger = LogManager.getLogger(FluidConfig.class);

    private static final ObjectMapper mapper;

    private static JsonNode root;

    private FluidConfig() {
      // Avoid direct instantiation.
    }

    static {
        mapper = new ObjectMapper()
            .enable(JsonParser.Feature.ALLOW_COMMENTS)
            .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
            .enable(JsonParser.Feature.ALLOW_TRAILING_COMMA)
            .enable(JsonParser.Feature.ALLOW_YAML_COMMENTS);
        load();
    }

    public  static synchronized void load() {
        String path = System.getProperty("fluid-config");
        if (path == null) {
            path = System.getenv("FLUID_CONFIG");
        }

        URL resource;
        if (path == null) {
            path = "fluid.yml";
            resource = FluidConfig.class.getClassLoader().getResource(path);
            if (resource == null) {
                path = "fluid.yaml";
                resource = FluidConfig.class.getClassLoader().getResource(path);
            }
        } else {
            try {
                resource = new File(path).toURI().toURL();
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Invalid config path: " + path, e);
            }
        }


        ObjectMapper yaml = new ObjectMapper(new YAMLFactory())
            .enable(JsonParser.Feature.ALLOW_COMMENTS)
            .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
            .enable(JsonParser.Feature.ALLOW_TRAILING_COMMA)
            .enable(JsonParser.Feature.ALLOW_YAML_COMMENTS);
        if (resource != null) {
            try {
                logger.info("Loading Fluid configuration from " + resource.toExternalForm());
                root = yaml.readValue(resource, ObjectNode.class);
            } catch (IOException e) {
                logger.error("Unable to read configuration from '" + resource.toExternalForm() + "'", e);
                throw new IllegalStateException("Unable to read the configuration", e);
            }
        } else {
            logger.warn("Unable to load the fluid configuration - no configuration found");
            root = NullNode.getInstance();
        }
    }

    public static ObjectMapper mapper() {
        return mapper;
    }

    public static String get(String path, String def) {
        return get(path)
            .map(JsonNode::asText).orElse(def);
    }

    public static String getString(String path) {
        return get(path, null);
    }

    public static int get(String path, int def) {
        return get(path)
            .filter(JsonNode::canConvertToInt)
            .map(JsonNode::asInt)
            .orElse(def);
    }

    public static long get(String path, long def) {
        return get(path)
            .filter(JsonNode::canConvertToLong)
            .map(JsonNode::asLong)
            .orElse(def);
    }

    public static boolean get(String path, boolean def) {
        return get(path)
            .filter(JsonNode::isBoolean)
            .map(JsonNode::asBoolean)
            .orElse(def);
    }


    public static double get(String path, double def) {
        return get(path)
            .filter(JsonNode::isDouble)
            .map(JsonNode::asDouble)
            .orElse(def);
    }

    public static ArrayNode getArray(String path) {
        return (ArrayNode) get(path)
            .filter(JsonNode::isArray)
            .orElse(null);
    }

    public  static synchronized Optional<JsonNode> get(String path) {
        Objects.requireNonNull(path, "The path must not be null");
        if (path.trim().isEmpty()) {
            throw new IllegalArgumentException("The path must not be empty");
        }
        JsonNode node = root.get(path);
        if (node != null) {
            return Optional.of(node);
        }

        if (path.contains(".")) {
            return traverse(root, path);
        }

        return Optional.empty();
    }

    private static synchronized Optional<JsonNode> traverse(JsonNode node, String path) {
        String[] segments = path.split("\\.");
        JsonNode current = node;
        for (String seg : segments) {
            JsonNode child = current.get(seg);
            if (child == null) {
                // Numeric ?
                int index = asInteger(seg);
                if (index >= 0) {
                    child = current.get(index);
                }
            }

            if (child == null) {
                return Optional.empty(); // Not found
            } else {
                current = child;
            }
        }
        return Optional.ofNullable(current);
    }

    private static int asInteger(String seg) {
        try {
            return Integer.valueOf(seg);
        } catch (Exception e) { // NOSONAR
          // Ignore exception
            return -1;
        }
    }
}
