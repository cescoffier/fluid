package me.escoffier.fluid;


import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;

public abstract class MyParentMediator {

    public abstract Source<String> source();

    public abstract Sink<String> sink();

    @Transformation
    public void transmute() {
        source().transform(String::toUpperCase).to(sink());
    }

}
