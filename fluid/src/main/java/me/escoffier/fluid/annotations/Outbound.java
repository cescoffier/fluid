package me.escoffier.fluid.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * On fields, in inject a {@link me.escoffier.fluid.models.Sink}.
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface Outbound {

    /**
     * @return the port name.
     */
    String value();

}
