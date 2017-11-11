package me.escoffier.fluid.annottions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Port {

    /**
     * @return the port name.
     */
    String value();

}
