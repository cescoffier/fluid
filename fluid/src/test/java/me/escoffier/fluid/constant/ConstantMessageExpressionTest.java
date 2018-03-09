package me.escoffier.fluid.constant;

import org.junit.Test;

import static me.escoffier.fluid.impl.DataExpressionFactories.requiredEventExpression;
import static org.assertj.core.api.Assertions.assertThat;

public class ConstantMessageExpressionTest {

    @Test
    public void shouldEvaluateNullConstantExpression() {
        Object value = requiredEventExpression(null).evaluate("event");
        assertThat(value).isNull();
    }

    @Test
    public void shouldEvaluateStringConstantExpression() {
        String constant = "constant";
        Object value = requiredEventExpression(constant).evaluate("event");
        assertThat(value).isEqualTo(constant);
    }

}
