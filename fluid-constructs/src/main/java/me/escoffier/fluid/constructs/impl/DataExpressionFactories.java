package me.escoffier.fluid.constructs.impl;

import me.escoffier.fluid.spi.DataExpression;
import me.escoffier.fluid.spi.DataExpressionFactory;

import java.util.Optional;
import java.util.ServiceLoader;

import static java.util.Optional.empty;

public class DataExpressionFactories {

    public static Optional<DataExpression> eventExpression(String expression) {
        for (DataExpressionFactory factory : ServiceLoader.load(DataExpressionFactory.class)) {
            if (factory.supports(expression)) {
                return Optional.of(factory.create(expression));
            }
        }
        return empty();
    }

    public static DataExpression requiredEventExpression(String expression) {
        return eventExpression(expression).orElseThrow(() -> new IllegalStateException("Unknown expression: " + expression));
    }

}