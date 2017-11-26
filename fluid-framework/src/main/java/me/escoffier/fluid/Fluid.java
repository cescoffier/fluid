package me.escoffier.fluid;

import io.reactivex.Flowable;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.annotations.Port;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.reflect.ReflectionHelper;
import me.escoffier.fluid.registry.FluidRegistry;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Fluid Framework entry point.
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
    Method[] methods = MethodUtils.getMethodsWithAnnotation(mediator.getClass(), Transformation.class, true, true);
    if (methods == null || methods.length == 0) {
      throw new IllegalArgumentException("Invalid object " + mediator + " - no transformation method found");
    }
    for (Method method : methods) {
      invoke(mediator, method);
    }
  }

  private Sink<Object> getSinkOrFail(String name) {
    Sink<Object> sink = FluidRegistry.sink(Objects.requireNonNull(name));
    if (sink == null) {
      throw new IllegalArgumentException("Unable to find the sink " + name);
    }
    return sink;
  }

  private Source<Object> getSourceOrFail(String name) {
    Source<Object> src = FluidRegistry.source(Objects.requireNonNull(name));
    if (src == null) {
      throw new IllegalArgumentException("Unable to find the sink " + name);
    }
    return src;
  }

  private void invoke(Object mediator, Method method) {
    method = ReflectionHelper.makeAccessibleIfNot(method);

    List<Object> values = new ArrayList<>();
    for (Parameter param : method.getParameters()) {
      Port port = param.getAnnotation(Port.class);
      if (port != null) {
        String name = port.value();
        if (param.getType().isAssignableFrom(Sink.class)) {
          values.add(getSinkOrFail(name));
        } else if (param.getType().isAssignableFrom(Source.class)) {
          values.add(getSourceOrFail(name));
        }
      } else {
        throw new IllegalArgumentException("Invalid parameter - one parameter of " + method.getName()
          + " is not annotated with @Port");
      }
    }

    try {
      method.invoke(mediator, values.toArray());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to invoke " + method.getName() + " from " + mediator.getClass()
        .getName(), e);
    }
  }

  private void inject(Object mediator) {
    List<Field> list = FieldUtils.getFieldsListWithAnnotation(mediator.getClass(), Port.class);
    for (Field field : list) {
      Port annotation = field.getAnnotation(Port.class);
      if (field.getType().isAssignableFrom(Sink.class)) {
        Sink<Object> sink = getSinkOrFail(annotation.value());
        ReflectionHelper.set(mediator, field, sink);
      } else if (field.getType().isAssignableFrom(Source.class)) {
        Source<Object> source = getSourceOrFail(annotation.value());
        ReflectionHelper.set(mediator, field, source);
      }
    }
  }




  public <T> Source<T> from(Flowable<T> flowable) {
    return Source.from(flowable);
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
