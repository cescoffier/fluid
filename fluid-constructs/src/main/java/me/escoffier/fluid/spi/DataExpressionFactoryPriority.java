package me.escoffier.fluid.spi;

public interface DataExpressionFactoryPriority {

    int STANDARD_PRIORITY = 0;

    int LOWER_PRIORITY = -100;

    int priority();

    static int priority(DataExpressionFactory factory) {
        return factory instanceof DataExpressionFactoryPriority ?
                ((DataExpressionFactoryPriority) factory).priority() : STANDARD_PRIORITY;
    }

}
