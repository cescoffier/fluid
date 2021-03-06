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

}
