package me.escoffier.fluid.config;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test fluid configuration.
 */
public class FluidConfigTest {

    private File out;

    @After
    public void tearDown() {
        FileUtils.deleteQuietly(out);
        System.clearProperty("fluid-config");
    }

    @Test
    public void loadDefault() throws IOException {
        // Prepare
        out = new File("target/test-classes/fluid.yml");
        File in = new File("src/test/resources/config/config-sample.yml");
        FileUtils.copyFile(in, out);

        FluidConfig.load();

        // Basic String queries
        assertThat(FluidConfig.getString("message")).isEqualTo("welcome");
        assertThat(FluidConfig.get("missing", "hello")).isEqualTo("hello");
        assertThat(FluidConfig.get("martin.missing", "miss")).isEqualTo("miss");
        assertThat(FluidConfig.getString("martin.missing")).isNull();

        // Objects
        assertThat(FluidConfig.get("martin")).isPresent();
        assertThat(FluidConfig.get("martin").get().fieldNames()).hasSize(4);
        assertThat(FluidConfig.getString("martin.job")).isEqualTo("Developer");
        assertThat(FluidConfig.get("martin.age", -1)).isEqualTo(45);

        // Array
        assertThat(FluidConfig.getArray("objects")).hasSize(2);
        assertThat(FluidConfig.getString("objects.0.martin.job")).isEqualTo("Developer");
        assertThat(FluidConfig.getString("objects.0.martin.skills.1")).isEqualTo("perl");
        assertThat(FluidConfig.get("objects.0.martin.missing")).isEmpty();
        assertThat(FluidConfig.get("objects.2")).isEmpty();
        assertThat(FluidConfig.getString("objects.1.tabitha.skills.2")).isEqualTo("erlang");

        // Dictionaries
        assertThat(FluidConfig.getString("dictionaries.martin.name")).isEqualTo("Martin D'vloper");
        assertThat(FluidConfig.getString("dictionaries.fruits.0")).isEqualTo("Apple");
        assertThat(FluidConfig.getArray("dictionaries.fruits")).hasSize(4);

        // Booleans
        assertThat(FluidConfig.get("booleans.create_key", false)).isTrue();
        assertThat(FluidConfig.get("booleans.needs_agent", true)).isFalse();
        assertThat(FluidConfig.get("booleans.knows_oop", false)).isTrue();
        assertThat(FluidConfig.get("booleans.likes_emacs", false)).isTrue();
        assertThat(FluidConfig.get("booleans.uses_cvs", true)).isFalse();
        assertThat(FluidConfig.get("booleans.missing", true)).isTrue();
        assertThat(FluidConfig.get("booleans.missing", false)).isFalse();

        // Multi-lines
        assertThat(FluidConfig.getString("include_newlines")).contains("exactly as you see\n");
        assertThat(FluidConfig.getString("ignore_newlines")).contains("this is really a single line of text");

        // Numbers
        assertThat(FluidConfig.get("an_integer", -1)).isEqualTo(25);
        assertThat(FluidConfig.get("missing", -1)).isEqualTo(-1);
        assertThat(FluidConfig.get("missing", -1L)).isEqualTo(-1L);
        assertThat(FluidConfig.get("an_integer", -1L)).isEqualTo(25L);
        assertThat(FluidConfig.get("a_double", -1.0)).isEqualTo(25.5);
        assertThat(FluidConfig.get("missing", -1.5)).isEqualTo(-1.5);

        // key format
        assertThat(FluidConfig.getString("key.with.dots")).isEqualTo("some value");
        assertThat(FluidConfig.getString("key-with-dashes")).isEqualTo("some value");
        assertThat(FluidConfig.getString("key with spaces")).isEqualTo("some value");

    }

    @Test
    public void loadDefaultWithYaml() throws IOException {
        // Prepare
        out = new File("target/test-classes/fluid.yaml");
        File in = new File("src/test/resources/config/config-sample.yml");
        FileUtils.copyFile(in, out);

        FluidConfig.load();

        // Basic String queries
        assertThat(FluidConfig.getString("message")).isEqualTo("welcome");
    }

    @Test
    public void loadFromSystemProperty() throws IOException {
        // Prepare
        System.setProperty("fluid-config", "src/test/resources/config/another.yml");

        FluidConfig.load();

        // Basic String queries
        assertThat(FluidConfig.getString("message")).isEqualTo("hello");
        assertThat(FluidConfig.getString("missing")).isNull();

        assertThat(FluidConfig.getArray("fruits")).hasSize(4);
    }

    @Test
    public void loadMissingConfig() {
        FluidConfig.load();

        assertThat(FluidConfig.get("message", "bar")).isEqualTo("bar");
    }

}