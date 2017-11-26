package me.escoffier.fluid.reflect;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class ReflectionHelper {

  private ReflectionHelper() {
    // Avoid direct instantiation
  }

  public static Method makeAccessibleIfNot(Method method) {
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
}
