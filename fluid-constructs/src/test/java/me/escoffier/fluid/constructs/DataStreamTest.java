package me.escoffier.fluid.constructs;


import org.junit.Test;

import java.util.List;

import static me.escoffier.fluid.constructs.Pair.pair;
import static me.escoffier.fluid.constructs.Tuple.tuple;
import static org.assertj.core.api.Assertions.assertThat;

public class DataStreamTest {


  @Test
  public void testOf() {
    DataStream<String> stream = DataStream.of(String.class);
    assertThat(stream.isConnectable()).isTrue();
    assertThat(stream.upstreams()).isEmpty();
    CacheSink<String> sink = new CacheSink<>();
    Source<String> source = Source.from("a", "b", "c");
    stream.connect(source);
    assertThat(stream.upstreams()).hasSize(1).contains((DataStream) source);
    stream.transformPayload(String::toUpperCase).to(sink);
    assertThat(sink.buffer).containsExactly("A", "B", "C");
  }

  @Test
  public void testUpstreamNavigabilityOnSequentialFlow() {
    ListSink<String> sink = Sink.list();
    Source<String> source = Source.from("a", "b", "c");
    DataStream<String> flow = source
      .transform(d -> d.with(d.payload().toUpperCase()));

    flow
      .to(sink);
    assertThat(sink.values()).containsExactly("A", "B", "C");

    assertThat(flow.upstreams()).hasSize(1).contains((DataStream) source);

    assertThat(source.upstreams()).isEmpty();
  }

  @Test
  public void tesDownstreamNavigabilityOnSequentialFlow() {
    ListSink<String> sink = Sink.list();
    Source<String> source = Source.from("a", "b", "c");
    DataStream<String> flow = source
      .transform(d -> d.with(d.payload().toUpperCase()));

    flow
      .to(sink);
    assertThat(sink.values()).containsExactly("A", "B", "C");

    assertThat(flow.downstreams()).hasSize(1);
    assertThat(flow.downstreams().iterator().next().downstreams()).isEmpty();
  }

  @Test
  public void testUpstreamNavigabilityOnFanInFlows() {
    // Merge
    ListSink<String> sink1 = Sink.list();
    Source<String> source1 = Source.from("a", "b", "c");
    Source<String> source2 = Source.from("d", "e", "f");

    DataStream<String> merge = source1.mergeWith(source2);
    merge.to(sink1);
    assertThat(sink1.values()).containsExactly("a", "b", "c", "d", "e", "f");
    assertThat(merge.upstreams()).hasSize(2).contains((DataStream) source1, (DataStream) source2);

    // Concat
    ListSink<String> sink2 = Sink.list();
    DataStream<String> concat = source1.concatWith(source2);
    concat.to(sink2);
    assertThat(sink2.values()).containsExactly("a", "b", "c", "d", "e", "f");
    assertThat(concat.upstreams()).hasSize(2).contains((DataStream) source1, (DataStream) source2);

    // Zip
    ListSink<Pair<String, String>> sink3 = Sink.list();
    DataStream<Pair<String, String>> zip = source1.zipWith(source2);
    zip.to(sink3);
    assertThat(sink3.values()).containsExactly(pair("a", "d"), pair("b", "e"), pair("c", "f"));
    assertThat(zip.upstreams()).hasSize(2).contains((DataStream) source1, (DataStream) source2);

    // Zip with tuple
    ListSink<Tuple> sink4 = Sink.list();
    Source<String> source3 = Source.from("g", "h", "i");
    DataStream<Tuple> zipTuple = source1.zipWith(source2, source3);
    zipTuple.to(sink4);
    assertThat(sink4.values()).containsExactly(tuple("a", "d", "g"), tuple("b", "e", "h"), tuple("c", "f", "i"));
    assertThat(zipTuple.upstreams()).hasSize(3).contains((DataStream) source1, (DataStream) source2, (DataStream) source3);
  }

  @Test
  public void testDownstreamNavigabilityOnFanInFlows() {
    // Merge
    ListSink<String> sink1 = Sink.list();
    Source<String> source1 = Source.from("a", "b", "c");
    Source<String> source2 = Source.from("d", "e", "f");

    DataStream<String> merge = source1.mergeWith(source2);
    merge.to(sink1);
    assertThat(sink1.values()).containsExactly("a", "b", "c", "d", "e", "f");
    assertThat(merge.downstreams()).hasSize(1);
    assertThat(merge.downstreams().iterator().next().downstreams()).isEmpty();


    // Concat
    ListSink<String> sink2 = Sink.list();
    DataStream<String> concat = source1.concatWith(source2);
    concat.to(sink2);
    assertThat(sink2.values()).containsExactly("a", "b", "c", "d", "e", "f");
    assertThat(concat.downstreams()).hasSize(1);
    assertThat(concat.downstreams().iterator().next().downstreams()).isEmpty();

    // Zip
    ListSink<Pair<String, String>> sink3 = Sink.list();
    DataStream<Pair<String, String>> zip = source1.zipWith(source2);
    zip.to(sink3);
    assertThat(sink3.values()).containsExactly(pair("a", "d"), pair("b", "e"), pair("c", "f"));
    assertThat(zip.downstreams()).hasSize(1);
    assertThat(zip.downstreams().iterator().next().downstreams()).isEmpty();


    // Zip with tuple
    ListSink<Tuple> sink4 = Sink.list();
    Source<String> source3 = Source.from("g", "h", "i");
    DataStream<Tuple> zipTuple = source1.zipWith(source2, source3);
    zipTuple.to(sink4);
    assertThat(sink4.values()).containsExactly(tuple("a", "d", "g"), tuple("b", "e", "h"), tuple("c", "f", "i"));
    assertThat(zipTuple.downstreams()).hasSize(1);
    assertThat(zip.downstreams().iterator().next().downstreams()).isEmpty();

    assertThat(source3.downstreams()).hasSize(1).contains(zipTuple);
    assertThat(source2.downstreams()).hasSize(4).contains(zip, concat, merge, zipTuple);
    assertThat(source1.downstreams()).hasSize(4).contains(zip, concat, merge, zipTuple);

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUpstreamNavigabilityOnFanOutFlows() {
    Source<Integer> source = Source.from(1, 2, 3, 4, 5, 6);
    // Conditional
    Pair<DataStream<Integer>, DataStream<Integer>> cond = source.branchOnPayload(i -> i % 2 == 0);
    assertThat(cond.left().upstreams()).hasSize(1).allSatisfy(d -> assertThat(d.upstreams()).contains((DataStream) source));
    assertThat(cond.right().upstreams()).hasSize(1).allSatisfy(d -> assertThat(d.upstreams()).contains((DataStream) source));

    // Branch
    List<DataStream<Integer>> branches = source.branchOnPayload(i -> i % 2 == 0, i -> i % 3 == 0);
    assertThat(branches.get(0).upstreams()).hasSize(1).allSatisfy(d -> assertThat(d.upstreams()).contains((DataStream) source));
    assertThat(branches.get(1).upstreams()).hasSize(1).allSatisfy(d -> assertThat(d.upstreams()).contains((DataStream) source));

    // Broadcast
    branches = source.broadcast(3);
    assertThat(branches.get(0).upstreams()).hasSize(1).allSatisfy(d -> assertThat(d.upstreams()).contains((DataStream) source));
    assertThat(branches.get(1).upstreams()).hasSize(1).allSatisfy(d -> assertThat(d.upstreams()).contains((DataStream) source));
    assertThat(branches.get(2).upstreams()).hasSize(1).allSatisfy(d -> assertThat(d.upstreams()).contains((DataStream) source));

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDownstreamNavigabilityOnFanOutFlows() {
    Source<Integer> source = Source.from(1, 2, 3, 4, 5, 6);
    // Conditional
    Pair<DataStream<Integer>, DataStream<Integer>> cond = source.branchOnPayload(i -> i % 2 == 0);
    assertThat(cond.left().downstreams()).hasSize(0);
    assertThat(cond.left().upstreams().iterator().next().downstreams()).contains(cond.left());
    assertThat(cond.right().downstreams()).hasSize(0);
    assertThat(cond.right().upstreams().iterator().next().downstreams()).contains(cond.right());

    // Branch
    List<DataStream<Integer>> branches = source.branchOnPayload(i -> i % 2 == 0, i -> i % 3 == 0);
    assertThat(branches.get(0).downstreams()).hasSize(0);
    assertThat(branches.get(0).upstreams().iterator().next().downstreams()).contains(branches.get(0));
    assertThat(branches.get(1).downstreams()).hasSize(0);
    assertThat(branches.get(1).upstreams().iterator().next().downstreams()).contains(branches.get(1));


    // Broadcast
    branches = source.broadcast(3);
    assertThat(branches.get(0).downstreams()).hasSize(0);
    assertThat(branches.get(0).upstreams().iterator().next().downstreams()).contains(branches.get(0));
    assertThat(branches.get(1).downstreams()).hasSize(0);
    assertThat(branches.get(1).upstreams().iterator().next().downstreams()).contains(branches.get(1));
    assertThat(branches.get(2).downstreams()).hasSize(0);
    assertThat(branches.get(2).upstreams().iterator().next().downstreams()).contains(branches.get(2));

  }

}
