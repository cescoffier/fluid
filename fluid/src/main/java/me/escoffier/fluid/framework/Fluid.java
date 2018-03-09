package me.escoffier.fluid.framework;

import io.reactivex.Flowable;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Outbound;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.models.Message;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.reflect.ReflectionHelper;
import me.escoffier.fluid.registry.FluidRegistry;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.reactivestreams.Publisher;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

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
      throw new IllegalArgumentException("Unable to find the source " + name);
    }
    return src;
  }

  private void invoke(Object mediator, Method method) {
    method = ReflectionHelper.makeAccessibleIfNot(method);

    List<Object> values = new ArrayList<>();
    for (Parameter param : method.getParameters()) {
      Inbound inbound = param.getAnnotation(Inbound.class);
      Outbound outbound = param.getAnnotation(Outbound.class);

      if (inbound != null) {
        String name = inbound.value();
        Source<Object> source = getSourceOrFail(name);
        Object inject = getSourceToInject(param.getType(), param.getParameterizedType(), source);
        values.add(inject);
      } else if (outbound != null) {
        String name = outbound.value();
        Sink<Object> sink = getSinkOrFail(name);
        values.add(sink);
      } else {
        throw new IllegalArgumentException("Invalid parameter - one parameter of " + method.getName()
          + " is not annotated with @Outbound or @Inbound");
      }
    }

    try {
      Class<?> returnType = method.getReturnType();
      Outbound outbound = method.getAnnotation(Outbound.class);
      if (returnType.equals(Void.TYPE)) {
        method.invoke(mediator, values.toArray());
      } else {
        if (outbound == null) {
          throw new IllegalStateException("The method " + method.getName() + " from "
            + mediator.getClass() + " needs to be annotated with @Outbound indicating the sink");
        } else {
          Sink<Object> sink = getSinkOrFail(outbound.value());
          Flowable<Object> flowable;
          if (returnType.isAssignableFrom(Flowable.class)) {
            flowable = (Flowable) method.invoke(mediator, values.toArray());
          } else if (returnType.isAssignableFrom(Publisher.class)) {
            flowable = Flowable.fromPublisher(
              (Publisher) method.invoke(mediator, values.toArray()));
          } else {
            throw new IllegalStateException("The method " + method.getName() + " from "
              + mediator.getClass() + " does not return a valid type");
          }

          Type type = method.getGenericReturnType();
          if (type instanceof ParameterizedType) {
            Type enclosed = ((ParameterizedType) type).getActualTypeArguments()[0];
            if (!enclosed.getTypeName().startsWith(Message.class.getName())) {
              flowable.flatMapCompletable(sink::dispatch).subscribe();
            } else {
              flowable
                .flatMapCompletable(d -> sink.dispatch((Message) d)).subscribe();
            }
          } else {
            flowable.flatMapCompletable(sink::dispatch).subscribe();
          }
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("Unable to invoke " + method.getName() + " from " + mediator.getClass()
        .getName(), e);
    }
  }

  private Object getSourceToInject(Class<?> clazz, Type type, Source<Object> source) {
    if (clazz.isAssignableFrom(Publisher.class)) {
      if (type instanceof ParameterizedType) {
        Type enclosed = ((ParameterizedType) type).getActualTypeArguments()[0];
        if (!enclosed.getTypeName().startsWith(Message.class.getName())) {
          return Flowable.fromPublisher(source).map(Message::payload);
        } else {
          return source;
        }
      } else {
        return source;
      }
    } else if (clazz.isAssignableFrom(Flowable.class)) {
      Flowable<Message<Object>> flowable = Flowable.fromPublisher(source);
      if (type instanceof ParameterizedType) {
        Type enclosed = ((ParameterizedType) type).getActualTypeArguments()[0];
        if (!enclosed.getTypeName().startsWith(Message.class.getName())) {
          return flowable.map(Message::payload);
        } else {
          return flowable;
        }
      } else {
        return flowable;
      }
    } else if (clazz.isAssignableFrom(Source.class)) {
      return source;
    }
    return source;
  }

  private void inject(Object mediator) {
    List<Field> list = FieldUtils.getFieldsListWithAnnotation(mediator.getClass(), Inbound.class);
    for (Field field : list) {
      Inbound annotation = field.getAnnotation(Inbound.class);
      Source<Object> source = getSourceOrFail(annotation.value());
      ReflectionHelper.set(mediator, field, getSourceToInject(field.getType(), field.getGenericType(), source));
    }

    list = FieldUtils.getFieldsListWithAnnotation(mediator.getClass(), Outbound.class);
    for (Field field : list) {
      Outbound annotation = field.getAnnotation(Outbound.class);
      if (field.getType().isAssignableFrom(Sink.class)) {
        Sink<Object> sink = getSinkOrFail(annotation.value());
        ReflectionHelper.set(mediator, field, sink);
      }
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
