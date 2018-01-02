package me.escoffier.fluid.constructs;

import io.reactivex.Completable;
import io.reactivex.internal.operators.completable.CompletableErrorSupplier;
import org.junit.Test;

import java.util.Date;

import static io.reactivex.Completable.complete;
import static io.reactivex.Completable.error;
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
    responseCallback(data).reply(timeResponse).subscribe();

    // Then
    assertThat(responseCallback.date.getTime()).isEqualTo(666);
  }

  @Test
  public void shouldPropagateReplyErrorDownstream() {
    // Given
    ErroringResponseCallback responseCallback = new ErroringResponseCallback();
    Data<String> data = new Data<>("payload").with(RESPONSE_CALLBACK, responseCallback);

    // When
    Completable replyResult = responseCallback(data).reply(new Object());

    // Then
    assertThat(replyResult).isInstanceOf(CompletableErrorSupplier.class);
  }

  static class DateResponseCallback implements ResponseCallback {

    Date date = new Date();

    @Override public Completable reply(Object response) {
      date.setTime((long)response);
      return complete();
    }

  }

  static class ErroringResponseCallback implements ResponseCallback {

    @Override public Completable reply(Object response) {
      return error(RuntimeException::new);
    }

  }

}
