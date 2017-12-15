package me.escoffier.fluid.constructs;

import org.junit.Test;

import java.util.Date;

import static me.escoffier.fluid.constructs.CommonHeaders.RESPONSE_CALLBACK;
import static me.escoffier.fluid.constructs.CommonHeaders.responseCallback;
import static org.assertj.core.api.Assertions.assertThat;

public class ResponseCallbackTest {

  @Test
  public void shouldExecuteResponseCallback() {
    // Given
    DateResponseCallback responseCallback = new DateResponseCallback();
    Data<String> data = new Data<>("payload").with(RESPONSE_CALLBACK, responseCallback);
    long timeResponse = 666;

    // When
    responseCallback(data).respond(timeResponse);

    // Then
    assertThat(responseCallback.date.getTime()).isEqualTo(666);
  }

  static class DateResponseCallback implements ResponseCallback {

    Date date = new Date();

    @Override public void respond(Object response) {
      date.setTime((long)response);
    }

  }

}
