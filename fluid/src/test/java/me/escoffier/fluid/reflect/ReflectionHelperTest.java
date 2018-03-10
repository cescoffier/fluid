package me.escoffier.fluid.reflect;

import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.registry.FluidRegistry;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Checks the internal behavior of the {@link ReflectionHelper} class.
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ReflectionHelperTest {

  @After
  public void tearDown() {
    FluidRegistry.unregisterSink("my-sink");
    FluidRegistry.unregisterSource("my-source");
  }

  public static class Test1 {
    String foo;
  }

  @Test
  public void testValidSet() throws NoSuchFieldException {
    Test1 test = new Test1();
    Field field = Test1.class.getDeclaredField("foo");
    ReflectionHelper.set(test, field, "hello");
    assertThat(test.foo).isEqualTo("hello");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSet() throws NoSuchFieldException {
    Test1 test = new Test1();
    Field field = Test1.class.getDeclaredField("foo");
    ReflectionHelper.set(test, field, 10);
  }

  public static class InvalidBecauseOfBadParams {
    @Function(outbound = "my-sink")
    public String function(String a) {
      return "invalid";
    }

    @Function(outbound = "my-sink")
    public String functionWithoutParam() {
      return "invalid";
    }

    @Transformation
    public void trans(Source<String> source) {
      // Invalid
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFunctionWithNotAnnotatedParameter() throws NoSuchMethodException {
    InvalidBecauseOfBadParams test = new InvalidBecauseOfBadParams();
    Method method = test.getClass().getMethod("function", String.class);
    ReflectionHelper.invokeFunction(test, method);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFunctionWithoutParam() throws NoSuchMethodException {
    InvalidBecauseOfBadParams test = new InvalidBecauseOfBadParams();
    Method method = test.getClass().getMethod("functionWithoutParam");
    ReflectionHelper.invokeFunction(test, method);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTransformationWithNotAnnotatedParameter() throws NoSuchMethodException {
    InvalidBecauseOfBadParams test = new InvalidBecauseOfBadParams();
    Method method = test.getClass().getMethod("trans", Source.class);
    ReflectionHelper.invokeTransformationMethod(test, method);
  }

  @Test
  public void testGettingAMissingSink() {
    FluidRegistry.register("my-sink", Sink.list());

    Sink<Object> sink = ReflectionHelper.getSinkOrFail("my-sink");
    assertThat(sink).isNotNull();

    try {
      ReflectionHelper.getSinkOrFail("missing");
      fail("The sink should be missing");
    } catch (IllegalArgumentException e) {
      // OK
    }
  }

  @Test
  public void testGettingAMissingSource() {
    FluidRegistry.register("my-source", Source.empty());

    Source<Object> source = ReflectionHelper.getSourceOrFail("my-source");
    assertThat(source).isNotNull();

    try {
      ReflectionHelper.getSourceOrFail("missing");
      fail("The source should be missing");
    } catch (IllegalArgumentException e) {
      // OK
    }
  }
}
