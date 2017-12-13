package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the common behavior of sources.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SourcesTest {


  @Test
  public void testMultiCast() {
    Source<Integer> source = Source.fromPayloads(Flowable.range(1, 10).replay().autoConnect());
    ListSink<Integer> sink1 = Sink.list();
    ListSink<Integer> sink2 = Sink.list();

    source.to(sink1);
    source.to(sink2);

    assertThat(sink1.values()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    assertThat(sink2.values()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }


}
