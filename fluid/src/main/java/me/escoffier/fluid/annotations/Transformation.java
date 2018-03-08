package me.escoffier.fluid.annotations;

import io.reactivex.Flowable;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Transformation {

  /**
   * Set the name of the outbound <em>sink</em>. This is used when the method returns a {@link Flowable} or a {@link org.reactivestreams.Publisher}
   */
  String outbound() default "";

}
