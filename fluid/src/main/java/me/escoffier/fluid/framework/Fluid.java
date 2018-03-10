package me.escoffier.fluid.framework;

import io.reactivex.Flowable;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.registry.FluidRegistry;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.*;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static me.escoffier.fluid.reflect.ReflectionHelper.inject;
import static me.escoffier.fluid.reflect.ReflectionHelper.invokeFunctionMethod;
import static me.escoffier.fluid.reflect.ReflectionHelper.invokeTransformationMethod;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Fluid {

  private final Vertx vertx;

  public Fluid() {
    this(Vertx.vertx());
  }

  public Fluid(Vertx vertx) {
    this.vertx = vertx;
    FluidRegistry.initialize(vertx);
  }

  public Fluid(io.vertx.core.Vertx vertx) {
    this(new Vertx(vertx));
  }

  public Fluid deploy(Consumer<Fluid> code) {
    code.accept(this);
    return this;
  }

  public Fluid deploy(Class<?> mediatorClass) {
    Object obj;
    try {
      obj = mediatorClass.newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to create a new instance from " + mediatorClass, e);
    }
    return deploy(obj);
  }

  public Fluid deploy(Object mediator) {
    Objects.requireNonNull(mediator, "Object must not be null");
    inject(mediator);
    execute(mediator);
    return this;
  }

  private void execute(Object mediator) {
    List<Method> tx = MethodUtils.getMethodsListWithAnnotation(mediator.getClass(), Transformation.class, true, true);
    List<Method> fn = MethodUtils.getMethodsListWithAnnotation(mediator.getClass(), Function.class, true, true);
    if (tx.isEmpty()  && fn.isEmpty()) {
      throw new IllegalArgumentException("Invalid object " + mediator + " - no transformation or function methods found");
    }

    for (Method method : tx) {
      invokeTransformationMethod(mediator, method);
    }

    for (Method method : fn) {
      invokeFunctionMethod(mediator, method);
    }
  }


  public <T> Source<T> from(Flowable<T> flowable) {
    return Source.fromPayloads(flowable);
  }

  public <T> Source<T> from(String name) {
    return FluidRegistry.source(name);
  }

  public <T> Sink<T> sink(String name) {
    return FluidRegistry.sink(name);
  }

  public Vertx vertx() {
    return vertx;
  }
}
