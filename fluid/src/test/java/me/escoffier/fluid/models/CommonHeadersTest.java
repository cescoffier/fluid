package me.escoffier.fluid.models;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static me.escoffier.fluid.models.CommonHeaders.*;
import static org.assertj.core.api.Assertions.assertThat;

public class CommonHeadersTest {

  private Data dataWithCommonHeaders = new Data<>("payload", ImmutableMap.of(KEY, "key", ORIGINAL, "original", ADDRESS, "address"));

  private Data dataWithoutCommonHeaders = new Data<>("payload");

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
