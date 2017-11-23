package me.escoffier.fluid.expression.constant;

import me.escoffier.fluid.spi.DataExpression;

public class ConstantDataExpression implements DataExpression {

    private final Object value;

    public ConstantDataExpression(Object value) {
        this.value = value;
    }

    @Override public Object evaluate(Object data) {
        return value;
    }

}