package me.escoffier.fluid.expression.constant;

import me.escoffier.fluid.spi.DataExpression;
import me.escoffier.fluid.spi.DataExpressionFactory;
import me.escoffier.fluid.spi.DataExpressionFactoryPriority;

public class ConstantDataExpressionFactory implements DataExpressionFactory, DataExpressionFactoryPriority {

    @Override public boolean supports(Object expression) {
        return true;
    }

    @Override public DataExpression create(Object expression) {
        return new ConstantDataExpression(expression);
    }

    @Override public int priority() {
        return LOWER_PRIORITY;
    }

}
