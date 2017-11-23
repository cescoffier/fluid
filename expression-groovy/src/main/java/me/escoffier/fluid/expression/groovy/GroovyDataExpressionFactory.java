package me.escoffier.fluid.expression.groovy;

import me.escoffier.fluid.spi.DataExpression;
import me.escoffier.fluid.spi.DataExpressionFactory;

public class GroovyDataExpressionFactory implements DataExpressionFactory {

    @Override
    public boolean supports(Object expression) {
        if(expression instanceof String) {
            String stringExpression = (String) expression;
            return stringExpression.startsWith("groovy:");
        }
        return false;
    }

    @Override
    public DataExpression create(Object expression) {
        String stringExpression = (String) expression;
        String expressionWithoutPrefix = stringExpression.substring(7);
        return new GroovyDataExpression(expressionWithoutPrefix);
    }

}