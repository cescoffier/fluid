package me.escoffier.fluid.spi;

public interface DataExpressionFactory {

    boolean supports(Object expression);

    DataExpression create(Object expression);

}
