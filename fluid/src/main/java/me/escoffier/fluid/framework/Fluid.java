package me.escoffier.fluid.framework;

import io.reactivex.Flowable;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.config.FluidConfig;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.registry.FluidRegistry;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static me.escoffier.fluid.reflect.ReflectionHelper.*;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Fluid {

  private final Vertx vertx;
  private FluidConfig config;

  /**
   * Creates a new instance of Fluid.
   *
   * @return a new instance of Fluid.
   */
  public static Fluid create() {
    return new Fluid();
  }

  /**
   * Creates a new instance of Fluid.
   *
   * @param vertx the instance of Vert.x to use. Must not be {@code null}
   * @return a new instance of Fluid using the given Vert.x instance.
   */
  public static Fluid create(Vertx vertx) {
    return new Fluid(vertx);
  }

  /**
   * Creates a new instance of Fluid.
   *
   * @param vertx the instance of Vert.x to use. Must not be {@code null}
   * @return a new instance of Fluid using the given Vert.x instance.
   */
  public static Fluid create(io.vertx.core.Vertx vertx) {
    return new Fluid(new Vertx(vertx));
  }

  /**
   * @deprecated - use {@link Fluid#create()}
   */
  @Deprecated
  public Fluid() {
    this(Vertx.vertx());
  }

  /**
   * @deprecated - use {@link Fluid#create()}
   */
  @Deprecated
  public Fluid(Vertx vertx) {
    this.vertx = vertx;
    config = new FluidConfig();
    FluidRegistry.initialize(vertx, config);
  }

  /**
   * @deprecated - use {@link Fluid#create()}
   */
  public Fluid(io.vertx.core.Vertx vertx) {
    this(new Vertx(vertx));
  }

  /**
   * Deploys a mediator implemented by the given consumer.
   *
   * @param code the consumer.
   * @return the current instance of {@link Fluid}
   */
  public Fluid deploy(Consumer<Fluid> code) {
    code.accept(this);
    return this;
  }

  /**
   * Deploys a mediator implemented by the given class.
   *
   * @param mediatorClass the implementation of the mediator, must not be {@code null}.
   * @return the current instance of {@link Fluid}
   */
  public Fluid deploy(Class<?> mediatorClass) {
    Object obj;
    try {
      obj = mediatorClass.newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to create a new instance from " + mediatorClass, e);
    }
    return deploy(obj);
  }

  /**
   * Deploys a mediator.
   *
   * @param mediator the mediator
   * @return the current instance of {@link Fluid}
   */
  public Fluid deploy(Object mediator) {
    Objects.requireNonNull(mediator, "Object must not be null");
    inject(mediator);
    execute(mediator);
    return this;
  }

  private void execute(Object mediator) {
    List<Method> tx = MethodUtils.getMethodsListWithAnnotation(mediator.getClass(), Transformation.class, true, true);
    List<Method> fn = MethodUtils.getMethodsListWithAnnotation(mediator.getClass(), Function.class, true, true);
    if (tx.isEmpty() && fn.isEmpty()) {
      throw new IllegalArgumentException("Invalid object " + mediator + " - no transformation or function methods found");
    }

    for (Method method : tx) {
      invokeTransformationMethod(mediator, method);
    }

    for (Method method : fn) {
      invokeFunction(mediator, method);
    }
  }

  /**
   * Creates a {@link Source} from the given {@link Flowable}.
   *
   * @param flowable the flowable, must not be {@code null}
   * @param <T>      the type of data.
   * @return the source
   */
  public <T> Source<T> from(Flowable<T> flowable) {
    return Source.fromPayloads(flowable);
  }

  /**
   * Looks for a registered source with the given name.
   *
   * @param name the name, must not be {@code null}
   * @param <T>  the type of data
   * @return the source, {@code null} if not found
   */
  public <T> Source<T> from(String name) {
    return FluidRegistry.source(name);
  }

  /**
   * Looks for a registered sink with the given name.
   *
   * @param name the name, must not be {@code null}
   * @param <T>  the type of data
   * @return the sink, {@code null} if not found
   */
  public <T> Sink<T> sink(String name) {
    return FluidRegistry.sink(name);
  }

  /**
   * @return the instance of Vert.x used by Fluid.
   */
  public Vertx vertx() {
    return vertx;
  }

  /**
   * @return the fluid configuration, cannot be changed once started.
   */
  public FluidConfig getConfig() {
    return config;
  }

  /**
   * Close operation to release resources
   */
  public void close() {
    vertx.close();
  }
}
