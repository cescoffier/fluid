package me.escoffier.fluid.constructs.impl;

import io.reactivex.Single;
import me.escoffier.fluid.constructs.DataStream;
import me.escoffier.fluid.constructs.ListSink;
import me.escoffier.fluid.constructs.Sink;
import me.escoffier.fluid.constructs.Source;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class DataStreamImplTest {


  @Test
  public void testTransformation() {
    ListSink<Integer> list = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .transform(i -> i + 1)
      .to(list);

    assertThat(list.values()).containsExactly(2, 3, 4, 5, 6);
  }

  @Test
  public void testTransformationFlow() {
    ListSink<Integer> list = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .transformFlow(flow -> flow.map(i -> i + 1))
      .to(list);

    assertThat(list.values()).containsExactly(2, 3, 4, 5, 6);
  }

  @Test
  public void testTransformer() {
    ListSink<Integer> list = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .transformWith(flow -> flow.map(i -> i + 1))
      .to(list);
    assertThat(list.values()).containsExactly(2, 3, 4, 5, 6);
  }

  @Test
  public void testThatWeCanRetrieveTheFlow() {
    List<Integer> list  = Source.from(1, 2, 3, 4, 5)
      .flow()
      .toList()
      .blockingGet();
    assertThat(list).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  public void testMergeWith() {
    Source<String> s1 = Source.from("a", "b", "c");
    Source<String> s2 = Source.from("d", "e", "f");
    Source<String> s3 = Source.from("g", "h");

    ListSink<String> list = Sink.list();
    // TODO Why do we have this generic issue
    s1.mergeWith(s2).to(list);

    assertThat(list.values()).containsExactly("a", "b", "c", "d", "e", "f");

    Random random = new Random();
    ListSink<String> list2 = Sink.list();
    s1.transformFlow(flow ->
      flow.delay(s ->
        Single.just(s)
          .delay(random.nextInt(100), TimeUnit.MILLISECONDS)
          .toFlowable()))
      .mergeWith(s2, s3)
      .to(list2);

    await().until(() -> list2.values().size() == 8);
    assertThat(list2.values()).contains("a", "b", "c", "d", "e", "f", "g", "h");
  }

  @Test
  public void testConcatWith() {
    Source<String> s1 = Source.from("a", "b", "c");
    Source<String> s2 = Source.from("d", "e", "f");
    Source<String> s3 = Source.from("g", "h");

    ListSink<String> list = Sink.list();
    // TODO Why do we have this generic issue
    s1.concatWith(s2).to(list);

    assertThat(list.values()).containsExactly("a", "b", "c", "d", "e", "f");

    ListSink<String> list2 = Sink.list();
    s1.transformFlow(flow ->
      flow.delay(s ->
        Single.just(s)
          .delay(10, TimeUnit.MILLISECONDS)
          .toFlowable())
    )
      .concatWith(s2, s3)
      .to(list2);

    await().until(() -> list2.values().size() == 8);
    assertThat(list2.values()).contains("a", "b", "c", "d", "e", "f", "g", "h");
  }



}
