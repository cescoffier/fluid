package me.escoffier.fluid.expression.groovy;

import groovy.lang.GroovyShell;
import me.escoffier.fluid.spi.DataExpression;

public class GroovyDataExpression implements DataExpression {

    private final String expression;

    public GroovyDataExpression(String expression) {
        this.expression = expression;
    }

    @Override public Object evaluate(Object data) {
        GroovyShell groovy = new GroovyShell();
        groovy.setVariable("data", data);
        return groovy.evaluate(expression);
    }

}