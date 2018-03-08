package me.escoffier.fluid.models;


import me.escoffier.fluid.impl.ListSink;
import org.junit.Test;

import static me.escoffier.fluid.models.Pair.pair;
import static me.escoffier.fluid.models.Tuple.tuple;
import static org.assertj.core.api.Assertions.assertThat;

public class SourceTest {


  @Test
  public void testMap() {
    ListSink<String> sink = Sink.list();
    Source<String> source = Source.from("a", "b", "c");
    Source<String> flow = source
      .map(d -> d.with(d.payload().toUpperCase()));

    flow
      .to(sink);
    assertThat(sink.values()).containsExactly("A", "B", "C");
  }

  @Test
  public void testUpstreamNavigabilityOnFanInFlows() {
    // Merge
    ListSink<String> sink1 = Sink.list();
    Source<String> source1 = Source.from("a", "b", "c");
    Source<String> source2 = Source.from("d", "e", "f");

    Source<String> merge = source1.mergeWith(source2);
    merge.to(sink1);
    assertThat(sink1.values()).containsExactly("a", "b", "c", "d", "e", "f");

    // Zip
    ListSink<Pair<String, String>> sink3 = Sink.list();
    Source<Pair<String, String>> zip = source1.zipWith(source2);
    zip.to(sink3);
    assertThat(sink3.values()).containsExactly(pair("a", "d"), pair("b", "e"), pair("c", "f"));

    // Zip with tuple
    ListSink<Tuple> sink4 = Sink.list();
    Source<String> source3 = Source.from("g", "h", "i");
    Source<Tuple> zipTuple = source1.zipWith(source2, source3);
    zipTuple.to(sink4);
    assertThat(sink4.values()).containsExactly(tuple("a", "d", "g"), tuple("b", "e", "h"), tuple("c", "f", "i"));
  }

  @Test
  public void testDownstreamNavigabilityOnFanInFlows() {
    // Merge
    ListSink<String> sink1 = Sink.list();
    Source<String> source1 = Source.from("a", "b", "c");
    Source<String> source2 = Source.from("d", "e", "f");

    Source<String> merge = source1.mergeWith(source2);
    merge.to(sink1);
    assertThat(sink1.values()).containsExactly("a", "b", "c", "d", "e", "f");


    // Zip
    ListSink<Pair<String, String>> sink3 = Sink.list();
    Source<Pair<String, String>> zip = source1.zipWith(source2);
    zip.to(sink3);
    assertThat(sink3.values()).containsExactly(pair("a", "d"), pair("b", "e"), pair("c", "f"));


    // Zip with tuple
    ListSink<Tuple> sink4 = Sink.list();
    Source<String> source3 = Source.from("g", "h", "i");
    Source<Tuple> zipTuple = source1.zipWith(source2, source3);
    zipTuple.to(sink4);
    assertThat(sink4.values()).containsExactly(tuple("a", "d", "g"), tuple("b", "e", "h"), tuple("c", "f", "i"));

  }

}
