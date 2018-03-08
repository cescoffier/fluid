package me.escoffier.fluid;


import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;

public abstract class MyParentMediator {

    public abstract Source<String> source();

    public abstract Sink<String> sink();

    @Transformation
    public void transmute() {
        source().mapItem(String::toUpperCase).to(sink());
    }

}
