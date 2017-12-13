package me.escoffier.fluid.constructs;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static me.escoffier.fluid.constructs.CommonHeaders.ADDRESS;
import static me.escoffier.fluid.constructs.CommonHeaders.KEY;
import static me.escoffier.fluid.constructs.CommonHeaders.ORIGINAL;
import static me.escoffier.fluid.constructs.CommonHeaders.address;
import static me.escoffier.fluid.constructs.CommonHeaders.addressOpt;
import static me.escoffier.fluid.constructs.CommonHeaders.key;
import static me.escoffier.fluid.constructs.CommonHeaders.keyOpt;
import static me.escoffier.fluid.constructs.CommonHeaders.originalOpt;
import static org.assertj.core.api.Assertions.assertThat;

public class CommonHeadersTest {

  Data dataWithCommonHeaders = new Data<>("payload", ImmutableMap.of(KEY, "key", ORIGINAL, "original", ADDRESS, "address"));

  Data dataWithoutCommonHeaders = new Data<>("payload");

  @Test
  public void shouldGetRequiredAddress() {
    assertThat(address(dataWithCommonHeaders)).isEqualTo("address");
  }

  @Test
  public void shouldHandleEmptyAddress() {
    assertThat(addressOpt(dataWithoutCommonHeaders)).isEmpty();
  }

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
