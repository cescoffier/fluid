package me.escoffier.fluid.models;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static me.escoffier.fluid.models.CommonHeaders.*;
import static org.assertj.core.api.Assertions.assertThat;

public class CommonHeadersTest {

  private Message messageWithCommonHeaders = new Message<>("payload", ImmutableMap.of(KEY, "key", ORIGINAL, "original", ADDRESS, "address"));

  private Message messageWithoutCommonHeaders = new Message<>("payload");

  @Test
  public void shouldGetRequiredAddress() {
    assertThat(address(messageWithCommonHeaders)).isEqualTo("address");
  }

  @Test
  public void shouldHandleEmptyAddress() {
    assertThat(addressOpt(messageWithoutCommonHeaders)).isEmpty();
  }

  @Test
  public void shouldGetRequiredKey() {
    assertThat(key(messageWithCommonHeaders)).isEqualTo("key");
  }

  @Test
  public void shouldHandleEmptyKey() {
    assertThat(keyOpt(messageWithoutCommonHeaders)).isEmpty();
  }

  @Test
  public void shouldGetRequiredOriginalData() {
    assertThat(CommonHeaders.<String>original(messageWithCommonHeaders)).isEqualTo("original");
  }

  @Test
  public void shouldHandleEmptyOriginal() {
    assertThat(originalOpt(messageWithoutCommonHeaders)).isEmpty();
  }

}
