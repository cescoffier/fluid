package me.escoffier.fluid.constructs;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static me.escoffier.fluid.constructs.CommonHeaders.KEY;
import static me.escoffier.fluid.constructs.CommonHeaders.ORIGINAL;
import static me.escoffier.fluid.constructs.CommonHeaders.key;
import static me.escoffier.fluid.constructs.CommonHeaders.keyOpt;
import static me.escoffier.fluid.constructs.CommonHeaders.originalOpt;
import static org.assertj.core.api.Assertions.assertThat;

public class CommonHeadersTest {

  Data dataWithCommonHeaders = new Data<>("payload", ImmutableMap.of(KEY, "key", ORIGINAL, "original"));

  Data dataWithoutCommonHeaders = new Data<>("payload");

  @Test
  public void shouldGetRequiredKey() {
    assertThat(key(dataWithCommonHeaders)).isEqualTo("key");
  }

  @Test
  public void shouldHandleEmptyKey() {
    assertThat(keyOpt(dataWithoutCommonHeaders)).isEmpty();
  }

  @Test
  public void shouldGetRequiredOriginalData() {
    assertThat(CommonHeaders.<String>original(dataWithCommonHeaders)).isEqualTo("original");
  }

  @Test
  public void shouldHandleEmptyOriginal() {
    assertThat(originalOpt(dataWithoutCommonHeaders)).isEmpty();
  }

}
