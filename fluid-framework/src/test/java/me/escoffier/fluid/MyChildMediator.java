package me.escoffier.fluid;

import me.escoffier.fluid.annotations.Port;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MyChildMediator extends MyParentMediator {

    @Port("input")
    Source<String> source;

    @Port("output")
    Sink<String> sink;

    @Override
    public Source<String> source() {
        return source;
    }

    @Override
    public Sink<String> sink() {
        return sink;
    }
}
