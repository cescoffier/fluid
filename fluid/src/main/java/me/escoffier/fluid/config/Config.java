package me.escoffier.fluid.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Config {

  private final Config parent;
  private final JsonNode node;

  private Config(Config parent, JsonNode node) {
    this.parent = parent;
    if (node == null) {
      this.node = FluidConfig.mapper().createObjectNode();
    } else {
      this.node = node;
    }
  }

  public Config(JsonNode node) {
    this(null, node);
  }

  /**
   * Constructor used mainly for testing purpose.
   *
   * @param json a non null json object
   * @throws IOException if the given json cannot be mapped to object node
   */
  public Config(JsonObject json) throws IOException {
    this(null, FluidConfig.mapper().readValue(json.encode(), JsonNode.class));
  }

  public boolean has(String path) {
    return get(path).isPresent();
  }

  public Config root() {
    Config current = this;
    while (current.parent != null) {
      current = current.parent;
    }
    return current;
  }

  public Optional<Config> parent() {
    return Optional.ofNullable(parent);
  }

  public boolean isRoot() {
    return parent == null;
  }

  public Optional<List<JsonNode>> getList(String path) {
    return get(path)
      .filter(JsonNode::isArray)
      .map(node -> {
        ArrayNode array = (ArrayNode) node;
        List<JsonNode> list = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
          list.add(array.get(i));
        }
        return list;
      });
  }

  public List<String> getStringList(String path) {
    return getList(path)
      .map(list -> list.stream().map(JsonNode::asText).collect(Collectors.toList()))
      .orElse(Collections.emptyList());
  }


  public List<Boolean> getBooleanList(String path) {
    return getList(path)
      .map(list -> list.stream()
        .filter(JsonNode::isBoolean)
        .map(JsonNode::asBoolean).collect(Collectors.toList()))
      .orElse(Collections.emptyList());
  }

  public List<Long> getLongList(String path) {
    return getList(path)
      .map(list -> list.stream()
        .filter(JsonNode::canConvertToLong)
        .map(JsonNode::asLong).collect(Collectors.toList()))
      .orElse(Collections.emptyList());
  }

  public List<Integer> getIntList(String path) {
    return getList(path)
      .map(list -> list.stream()
        .filter(JsonNode::canConvertToInt)
        .map(JsonNode::asInt).collect(Collectors.toList()))
      .orElse(Collections.emptyList());
  }

  public List<Double> getDoubleList(String path) {
    return getList(path)
      .map(list -> list.stream()
        .map(JsonNode::asDouble).collect(Collectors.toList()))
      .orElse(Collections.emptyList());
  }

  public List<Config> getConfigList(String path) {
    String[] segments = path.split("\\.");
    JsonNode current = node;
    Config parent = this;
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
        return Collections.emptyList();
      } else {
        parent = new Config(parent, current);
        current = child;
      }
    }
    if (current == null) {
      return Collections.emptyList();
    } else {
      if (!current.isArray()) {
        return Collections.emptyList();
      } else {
        ArrayNode array = (ArrayNode) current;
        List<Config> config = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
          config.add(new Config(parent, array.get(i)));
        }
        return config;
      }
    }
  }

  public String getString(String path, String def) {
    return getString(path).orElse(def);
  }

  public Optional<String> getString(String path) {
    return get(path).map(JsonNode::asText);
  }

  public Optional<Integer> getInt(String path) {
    return get(path)
      .filter(JsonNode::canConvertToInt)
      .map(JsonNode::asInt);
  }

  public int getInt(String path, int def) {
    return getInt(path).orElse(def);
  }

  public Optional<Long> getLong(String path) {
    return get(path)
      .filter(JsonNode::canConvertToLong)
      .map(JsonNode::asLong);
  }

  public long getLong(String path, long def) {
    return getLong(path).orElse(def);
  }

  public Optional<Boolean> getBoolean(String path) {
    return get(path)
      .filter(JsonNode::isBoolean)
      .map(JsonNode::asBoolean);
  }

  public boolean getBoolean(String path, boolean def) {
    return getBoolean(path).orElse(def);
  }

  public Optional<Double> getDouble(String path) {
    return get(path)
      .filter(JsonNode::isDouble)
      .map(JsonNode::asDouble);
  }

  public double getDouble(String path, double def) {
    return getDouble(path).orElse(def);
  }

  public <T> T as(Class<T> clazz) {
    try {
      return FluidConfig.mapper().treeToValue(node, Objects.requireNonNull(clazz));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to map the configuration to an instance of "
        + clazz.getName(), e);
    }
  }


  public synchronized Optional<JsonNode> get(String path) {
    Objects.requireNonNull(path, "The path must not be null");
    if (path.trim().isEmpty()) {
      throw new IllegalArgumentException("The path must not be empty");
    }
    JsonNode node = this.node.get(path);
    if (node != null) {
      return Optional.of(node);
    }

    if (path.contains(".")) {
      return traverse(this.node, path);
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


  public Optional<Config> getConfig(String path) {
    String[] segments = path.split("\\.");
    JsonNode current = node;
    Config parent = this;
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
        parent = new Config(parent, current);
        current = child;
      }
    }
    if (current == null) {
      return Optional.empty();
    } else {
      return Optional.of(new Config(parent, current));
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Config && ((Config) obj).node.equals(node);
  }

  @Override
  public int hashCode() {
    return node.hashCode();
  }

  public JsonNode node() {
    return node;
  }

  public Iterator<String> names() {
    return node.fieldNames();
  }

}
