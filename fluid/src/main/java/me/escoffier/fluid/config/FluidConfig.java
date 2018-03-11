package me.escoffier.fluid.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Utilities to read the Fluid configuration.
 * <p>
 * The configuration is read from the following location:
 * * `fluid-config` system property
 * * `FLUID_CONFIG` environment variable
 * * `fluid.yml` in the classpath
 * * `fluid.yaml` in the classpath
 */
public class FluidConfig extends Config {

  private static final Logger logger = LogManager.getLogger(FluidConfig.class);

  private static final ObjectMapper mapper;

  public FluidConfig() {
    super(load());
  }

  static {
    mapper = new ObjectMapper()
      .enable(JsonParser.Feature.ALLOW_COMMENTS)
      .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
      .enable(JsonParser.Feature.ALLOW_TRAILING_COMMA)
      .enable(JsonParser.Feature.ALLOW_YAML_COMMENTS);
  }

  private static synchronized JsonNode load() {
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
        return yaml.readValue(resource, ObjectNode.class);
      } catch (IOException e) {
        logger.error("Unable to read configuration from '" + resource.toExternalForm() + "'", e);
        throw new IllegalStateException("Unable to read the configuration", e);
      }
    } else {
      logger.warn("Unable to load the fluid configuration - no configuration found");
      return NullNode.getInstance();
    }
  }

  public static ObjectMapper mapper() {
    return mapper;
  }

}
