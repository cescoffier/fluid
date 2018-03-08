package me.escoffier.fluid.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Injects a port. Depending on the type, it will looks for a {@link me.escoffier.fluid.constructs.Sink} or a
 * {@link me.escoffier.fluid.constructs.Source}.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface Port {

    /**
     * @return the port name.
     */
    String value();

}
