package me.escoffier.fluid.expression.groovy;

import org.junit.Test;

import static me.escoffier.fluid.constructs.impl.DataExpressionFactories.requiredEventExpression;
import static org.assertj.core.api.Assertions.assertThat;

public class GroovyDataExpressionTest {

    @Test
    public void shouldEvaluateConstant() {
        String result = (String) requiredEventExpression("groovy:'foo'").evaluate(null);
        assertThat(result).isEqualTo("foo");
    }

    @Test
    public void shouldEvaluateData() {
        String result = (String) requiredEventExpression("groovy:data").evaluate("foo");
        assertThat(result).isEqualTo("foo");
    }

    @Test
    public void shouldEvaluateIntegerOperations() {
        int result = (int) requiredEventExpression("groovy:data * 2").evaluate(2);
        assertThat(result).isEqualTo(4);
    }

}
