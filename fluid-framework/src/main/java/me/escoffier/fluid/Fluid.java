package me.escoffier.fluid;

import io.reactivex.Flowable;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.annotations.Port;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Sinks;
import me.escoffier.fluid.constructs.Source;
import me.escoffier.fluid.constructs.Sources;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
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

        Sources.load(vertx);
        Sinks.load(vertx);
    }

    public Fluid(io.vertx.core.Vertx vertx) {
        this(new Vertx(vertx));
    }

    public Fluid deploy(Consumer<Fluid> code) {
        code.accept(this);
        return this;
    }

    public Fluid deploy(Class<?> mediatorClass) {
        Object obj = null;
        try {
            obj = mediatorClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Unable to create a new instance from " + mediatorClass, e);
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
        if (methods == null  || methods.length == 0) {
            throw new IllegalArgumentException("Invalid object " + mediator + " - no transformation method found");
        }
        for (Method method : methods) {
            invoke(mediator, method);
        }
    }

    private void invoke(Object mediator, Method method) {
        if (!method.isAccessible()) {
            method.setAccessible(true);
        }

        Parameter[] parameters = method.getParameters();
        List<Object>  values = new ArrayList<>();
        for (Parameter param : parameters) {
            Port port = param.getAnnotation(Port.class);
            if (port != null) {
                String name = port.value();
                if (param.getType().isAssignableFrom(Sink.class)) {
                    Sink<Object> sink = Sinks.get(name);
                    if (sink == null) {
                        throw new IllegalArgumentException("Unable to find the sink " + name);
                    } else {
                        values.add(sink);
                    }
                } else if (param.getType().isAssignableFrom(Source.class)) {
                    Source<Object> source = Sources.get(name);
                    if (source == null) {
                        throw new IllegalArgumentException("Unable to find the source " + name);
                    } else {
                        values.add(source);
                    }
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
                Sink<Object> sink = Sinks.get(annotation.value());
                if (sink == null) {
                    throw new IllegalArgumentException("Unable to find the sink " + annotation.value());
                } else {
                    set(mediator, field, sink);
                }
            } else if (field.getType().isAssignableFrom(Source.class)) {
                Source<Object> source = Sources.get(annotation.value());
                if (source == null) {
                    throw new IllegalArgumentException("Unable to find the source " + annotation.value());
                } else {
                    set(mediator, field, source);
                }
            }
        }
    }

    private void set(Object mediator, Field field, Object source) {
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


    public <T> Source<T> from(Flowable<T> flowable) {
        return Source.from(flowable);
    }

    public <T> Source<T> from(String name) {
        return Sources.get(name);
    }
    
    public <T> Sink<T> sink(String name) {
        return Sinks.get(name);
    }

    public Vertx vertx() {
        return vertx;
    }
}
