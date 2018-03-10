package me.escoffier.fluid.reflect;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import me.escoffier.fluid.annotations.Function;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Outbound;
import me.escoffier.fluid.models.Message;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import me.escoffier.fluid.registry.FluidRegistry;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.reactivestreams.Publisher;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ReflectionHelper {

  private ReflectionHelper() {
    // Avoid direct instantiation
  }

  private static Method makeAccessibleIfNot(Method method) {
    if (!Objects.requireNonNull(method).isAccessible()) {
      method.setAccessible(true);
    }
    return method;
  }

  public static void set(Object mediator, Field field, Object source) {
    if (!field.isAccessible()) {
      field.setAccessible(true);
    }
    try {
      field.set(mediator, source);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Unable to set field " + field.getName() + " from " + mediator.getClass()
        .getName() + " to " + source, e);
    }
  }

  private static Completable propagateResult(Object result, Sink<Object> sink) {
    if (result instanceof Publisher) {
      return Flowable.fromPublisher((Publisher) result).flatMapCompletable(d -> {
        if (d instanceof Message) {
          return sink.dispatch((Message) d);
        } else {
          return sink.dispatch(d);
        }
      });
    } else if (result instanceof Message) {
      return sink.dispatch((Message) result);
    } else { // Payload
      return sink.dispatch(result);
    }
  }

  public static void invokeFunction(Object mediator, Method method) {
    method = ReflectionHelper.makeAccessibleIfNot(method);

    List<Flowable<Object>> sources = getFlowableForParameters(method);

    Function function = method.getAnnotation(Function.class);
    Sink<Object> sink = null;
    if (function.outbound().length() != 0) {
      sink = getSinkOrFail(function.outbound());
    }

    Method methodToBeInvoked = method;
    Sink<Object> theSink = sink;

    Flowable<Optional<Object>> result;
    if (sources.size() == 1) {
      result = sources.get(0)
        .map(item -> Optional.ofNullable(methodToBeInvoked.invoke(mediator, item)));
    } else {
      result = Flowable.zip(sources, args -> args)
        .map(args -> Optional.ofNullable(methodToBeInvoked.invoke(mediator, args)));
    }

    result
      .flatMapCompletable(maybeResult -> {
          if (! maybeResult.isPresent()) {
            // We didn't get a result
            return Completable.complete();
          } else {
            return propagateResult(maybeResult.get(), theSink);
          }
      })
      .doOnError(Throwable::printStackTrace) // TODO improve error reporting
      .subscribe();
  }

  private static List<Flowable<Object>> getFlowableForParameters(Method method) {
    List<Flowable<Object>> sources = new ArrayList<>();

    if (method.getParameterCount() == 0) {
      throw new IllegalStateException("Invalid number of parameter for the function - you need at " +
        "least one parameter");
    }

    for (Parameter param : method.getParameters()) {
      Inbound inbound = param.getAnnotation(Inbound.class);
      if (inbound == null) {
        throw new IllegalStateException("Invalid function method - all parameters must " +
          "be annotated with @Inbound");
      }

      String name = inbound.value();
      Source<Object> source = getSourceOrFail(name);

      if (!param.getType().isAssignableFrom(Message.class)) {
        sources.add(source.asFlowable().map(Message::payload));
      } else {
        sources.add(source.asFlowable().cast(Object.class));
      }
    }
    return sources;
  }


  public static void invokeTransformationMethod(Object mediator, Method method) {
    method = ReflectionHelper.makeAccessibleIfNot(method);
    List<Object> values = getParameterFromTransformationMethod(method);

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
          if (Publisher.class.isAssignableFrom(returnType)) {
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
              flowable.flatMapCompletable(sink::dispatch)
                .doOnError(Throwable::printStackTrace) // TODO improve error reporting
                .subscribe();
            } else {
              flowable
                .flatMapCompletable(d -> sink.dispatch((Message) d))
                .doOnError(Throwable::printStackTrace) // TODO improve error reporting
                .subscribe();
            }
          } else {
            flowable.flatMapCompletable(sink::dispatch)
              .doOnError(Throwable::printStackTrace) // TODO improve error reporting
              .subscribe();
          }
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("Unable to invoke " + method.getName() + " from " + mediator.getClass()
        .getName(), e);
    }
  }

  private static List<Object> getParameterFromTransformationMethod(Method method) {
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
    return values;
  }

  public static Object getSourceToInject(Class<?> clazz, Type type, Source<Object> source) {
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

  public static void inject(Object mediator) {
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


  private static Sink<Object> getSinkOrFail(String name) {
    Sink<Object> sink = FluidRegistry.sink(Objects.requireNonNull(name));
    if (sink == null) {
      throw new IllegalArgumentException("Unable to find the sink " + name);
    }
    return sink;
  }

  private static Source<Object> getSourceOrFail(String name) {
    Source<Object> src = FluidRegistry.source(Objects.requireNonNull(name));
    if (src == null) {
      throw new IllegalArgumentException("Unable to find the source " + name);
    }
    return src;
  }
}
