package me.escoffier.fluid.config;

import me.escoffier.fluid.framework.Fluid;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test fluid configuration.
 */
public class FluidConfigTest {

  private File out;

  private Fluid fluid;


  @After
  public void tearDown() {
    if (fluid != null) {
      fluid.close();
    }

    FileUtils.deleteQuietly(out);
    System.clearProperty("fluid-config");
  }

  @SuppressWarnings({"ResultOfMethodCallIgnored", "ConstantConditions"})
  @Test
  public void loadDefault() throws IOException {
    // Prepare
    out = new File("target/test-classes/fluid.yml");
    File in = new File("src/test/resources/config/config-sample.yml");
    FileUtils.copyFile(in, out);
    fluid = new Fluid();
    FluidConfig config = fluid.getConfig();

    // Basic String queries
    assertThat(config.getString("message")).get().isEqualTo("welcome");
    assertThat(config.getString("missing", "hello")).isEqualTo("hello");
    assertThat(config.getString("martin.missing", "miss")).isEqualTo("miss");
    assertThat(config.getString("martin.missing")).isEmpty();

    // Objects
    assertThat(config.get("martin")).isPresent();
    assertThat(config.get("martin").get().fieldNames()).hasSize(4);
    assertThat(config.getString("martin.job")).get().isEqualTo("Developer");
    assertThat(config.getInt("martin.age", -1)).isEqualTo(45);

    // Array
    assertThat(config.getStringList("objects")).hasSize(2);
    assertThat(config.getString("objects.0.martin.job")).get().isEqualTo("Developer");
    assertThat(config.getString("objects.0.martin.skills.1")).get().isEqualTo("perl");
    assertThat(config.getList("objects.0.martin.missing")).isEmpty();
    assertThat(config.getInt("objects.2")).isEmpty();
    assertThat(config.getString("objects.1.tabitha.skills.2")).get().isEqualTo("erlang");

    // Dictionaries
    assertThat(config.getString("dictionaries.martin.name")).get().isEqualTo("Martin D'vloper");
    assertThat(config.getString("dictionaries.fruits.0")).get().isEqualTo("Apple");
    assertThat(config.getList("dictionaries.fruits")).get().asList().hasSize(4);

    // Booleans
    assertThat(config.getBoolean("booleans.create_key", false)).isTrue();
    assertThat(config.getBoolean("booleans.needs_agent", true)).isFalse();
    assertThat(config.getBoolean("booleans.knows_oop", false)).isTrue();
    assertThat(config.getBoolean("booleans.likes_emacs", false)).isTrue();
    assertThat(config.getBoolean("booleans.uses_cvs", true)).isFalse();
    assertThat(config.getBoolean("booleans.missing", true)).isTrue();
    assertThat(config.getBoolean("booleans.missing", false)).isFalse();
    assertThat(config.getBoolean("booleans.create_key")).contains(true);
    assertThat(config.getBoolean("booleans.needs_agent")).contains(false);
    assertThat(config.getBoolean("booleans.missing")).isEmpty();

    // Multi-lines
    assertThat(config.getString("include_newlines")).get().asString().contains("exactly as you see\n");
    assertThat(config.getString("ignore_newlines")).get().asString().contains("this is really a single line of text");

    // Numbers
    assertThat(config.getInt("an_integer", -1)).isEqualTo(25);
    assertThat(config.getInt("missing", -1)).isEqualTo(-1);
    assertThat(config.getLong("missing", -1L)).isEqualTo(-1L);
    assertThat(config.getLong("missing")).isEmpty();
    assertThat(config.getLong("a_long")).contains(2500000L);
    assertThat(config.getLong("an_integer", -1L)).isEqualTo(25L);
    assertThat(config.getDouble("a_double", -1.0)).isEqualTo(25.5);
    assertThat(config.getDouble("a_double")).contains(25.5);
    assertThat(config.getDouble("missing", -1.5)).isEqualTo(-1.5);

    // key format
    assertThat(config.getString("key.with.dots")).get().isEqualTo("some value");
    assertThat(config.getString("key-with-dashes")).get().isEqualTo("some value");
    assertThat(config.getString("key with spaces")).get().isEqualTo("some value");

    // Get sub-object
    Optional<Config> martin = config.getConfig("martin");
    assertThat(martin).isNotEmpty();
    assertThat(martin.get().root()).isEqualTo(config);
    assertThat(martin.get().isRoot()).isFalse();
    assertThat(martin.get().root().isRoot()).isTrue();
    assertThat(config.isRoot()).isTrue();
    assertThat(martin.get().getString("job")).get().isEqualTo("Developer");
    assertThat(martin.get().getInt("age", -1)).isEqualTo(45);

    // nested / level1 / level2
    Optional<Config> nested = config.getConfig("nested");
    Optional<Config> level1 = config.getConfig("nested.level1");
    Optional<Config> level2 = config.getConfig("nested.level1.level2");
    assertThat(nested.isPresent()).isTrue();
    assertThat(level1.isPresent()).isTrue();
    assertThat(level2.isPresent()).isTrue();
    assertThat(nested.get().isRoot()).isFalse();
    assertThat(level1.get().isRoot()).isFalse();
    assertThat(level2.get().isRoot()).isFalse();
    assertThat(level1.get().parent()).get().isEqualTo(nested.get());
    assertThat(level2.get().parent()).get().isEqualTo(level1.get());
    assertThat(nested.get().parent()).get().isEqualTo(config);
    assertThat(level2.get().root()).isEqualTo(config);
    assertThat(level1.get().root()).isEqualTo(config);
    assertThat(nested.get().root()).isEqualTo(config);
    assertThat(nested.get().getConfig("missing")).isEmpty();
    assertThat(config.getConfig("nested.array.1")).isNotEmpty();
    assertThat(config.getConfig("nested.array.3")).isEmpty();
    assertThat(config.getConfig("nested.array.1").get().parent().get().parent())
      .contains(nested.get());

    // has
    assertThat(config.has("martin")).isTrue();
    assertThat(config.has("clement")).isFalse();
    assertThat(config.has("nested.level1.level2")).isTrue();
    assertThat(config.has("nested.level1.message")).isTrue();
    assertThat(config.has("nested.level1.level2.message")).isTrue();
    assertThat(config.has("nested.level1.missing")).isFalse();
    assertThat(config.has("objects.0.martin.job")).isTrue();
    assertThat(config.has("objects.1.martin.job")).isFalse();


    // Lists
    assertThat(config.getBooleanList("lists.bools")).containsExactly(true, false, true, false);
    assertThat(config.getLongList("lists.longs")).containsExactly(1000L, 1001L, 1002L);
    assertThat(config.getConfig("lists").map(c -> c.getLongList("longs"))).get().asList()
      .containsExactly(1000L, 1001L, 1002L);
    assertThat(config.getConfig("lists").map(c -> c.getDoubleList("doubles"))).get().asList()
      .containsExactly(10.0, 5.0, 25.3, 42.0);
    assertThat(config.getConfig("lists").map(c -> c.getConfigList("configs"))).get().asList()
      .hasSize(2)
      .allSatisfy(c -> ((Config) c).parent().isPresent())
      .allSatisfy(c -> ((Config) c).parent().get().parent().get().isRoot());

    // Test null path
    try {
      config.get(null);
    } catch (NullPointerException e) {
      // Error caught, we are ok
    }

    try {
      config.getConfig("lists").get().get(null);
    } catch (NullPointerException e) {
      // Error caught, we are ok
    }

    // Mapping to object
    assertThat(config.getConfig("lists.configs.0")).isNotEmpty();
    assertThat(config.getConfig("lists.configs.0").get().as(Person.class)
      .getFirstName()).isEqualTo("Bob");
    assertThat(config.getConfig("lists.configs.1").get().as(Person.class)
      .getFirstName()).isEqualTo("Bill");

    assertThat(config.hashCode()).isEqualTo(config.node().hashCode());
  }

  @Test
  public void loadDefaultWithYaml() throws IOException {
    // Prepare
    out = new File("target/test-classes/fluid.yaml");
    File in = new File("src/test/resources/config/config-sample.yml");
    FileUtils.copyFile(in, out);

    fluid = new Fluid();
    assertThat(fluid.getConfig().getString("message")).get().isEqualTo("welcome");
  }

  @Test
  public void loadFromSystemProperty() throws IOException {
    // Prepare
    System.setProperty("fluid-config", "src/test/resources/config/another.yml");

    fluid = new Fluid();

    // Basic String queries
    assertThat(fluid.getConfig().getString("message")).get().isEqualTo("hello");
    assertThat(fluid.getConfig().getString("missing")).isEmpty();
    assertThat(fluid.getConfig().getList("fruits")).get().asList().hasSize(4);
  }

  @Test
  public void loadMissingConfig() {
    fluid = new Fluid();
    assertThat(fluid.getConfig().getString("message", "bar")).isEqualTo("bar");
  }

  @Test
  public void testEmpty() {
    assertThat(Config.empty().names()).isEmpty();
  }


}
