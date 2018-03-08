package me.escoffier.fluid.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Injects a {@link me.escoffier.fluid.models.Source}, {@link org.reactivestreams.Publisher} or {@link io.reactivex.Flowable}.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface Inbound {

    /**
     * @return the port name.
     */
    String value();

}
