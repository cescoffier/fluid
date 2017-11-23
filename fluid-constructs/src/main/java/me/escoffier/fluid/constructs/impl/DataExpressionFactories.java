package me.escoffier.fluid.constructs.impl;

import me.escoffier.fluid.spi.DataExpression;
import me.escoffier.fluid.spi.DataExpressionFactory;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Optional.empty;
import static me.escoffier.fluid.spi.DataExpressionFactoryPriority.priority;

public class DataExpressionFactories {

    public static Optional<DataExpression> eventExpression(String expression) {
        Set<DataExpressionFactory> factories = new TreeSet<>((firstFactory, secondFactory) -> priority(secondFactory) - priority(firstFactory));
        ServiceLoader.load(DataExpressionFactory.class).iterator().forEachRemaining(factories::add);
        for (DataExpressionFactory factory : factories) {
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