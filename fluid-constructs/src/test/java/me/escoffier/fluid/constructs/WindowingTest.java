package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class WindowingTest {

  @Test
  public void simple() {
    List<Data<String>> watermarks = new ArrayList<>();
    ListSink<String> list = Sink.list();
    Source.from("a", "b", "c", "d", "e")
      .withWindow(Windowing.bySize(2))
      .onData(v -> {
        if (Watermark.isWatermark(v)) {
          watermarks.add(v);
        }
      })
      .to(list);

    assertThat(list.data()).hasSize(5);
    assertThat(watermarks).hasSize(3);
    List<Window> windows = watermarks.stream()
      .map(d -> ((Window) d.get("fluid-window")))
      .collect(Collectors.toList());
    assertThat(windows).hasSize(3);
    assertThat(list.values()).containsExactly("a", "b", "c", "d", "e");
    assertThat(list.data()).allSatisfy(data -> {
      Window window = data.get("fluid-window");
      assertThat(window).isNotNull();
      assertThat(windows).contains(window);
    });
  }

  @Test
  public void testWindowingWithMerge() {
    ListSink<String> sink = Sink.list();

    DataStream<String> stream1 = Source.from("a", "b", "c", "d", "e")
      .withWindow(Windowing.bySize(2))
      .transformPayload(String::toUpperCase);

    DataStream<String> stream2 = Source.from("Z", "Y", "X")
      .withWindow(Windowing.bySize(3))
      .transformPayload(String::toLowerCase);

    stream1.mergeWith(stream2).to(sink);

    assertThat(sink.values()).contains("A", "B", "C", "D", "E", "z", "y", "x");
  }

  @Test
  public void testWindowingWithMultiMerge() {
    ListSink<String> sink = Sink.list();

    DataStream<String> stream1 = Source.from("a", "b", "c", "d", "e")
      .withWindow(Windowing.bySize(2))
      .transformPayload(String::toUpperCase);

    DataStream<String> stream2 = Source.from("Z", "Y", "X")
      .withWindow(Windowing.bySize(3))
      .transformPayload(String::toLowerCase);

    DataStream<String> stream3 = Source.from("L", "M", "O");

    stream1.mergeWith(stream2, stream3).to(sink);

    assertThat(sink.values()).contains("A", "B", "C", "D", "E", "z", "y", "x", "L", "M", "O");
  }

  @Test
  public void testWithTransform() {
    ListSink<String> sink = Sink.list();
    List<ControlData> control = new ArrayList<>();

    Source.from("a", "b", "c", "d", "e", "g", "h", "i")
      .withWindow(Windowing.bySize(2))
      .transform(f -> f.with(f.payload().toUpperCase())
      )
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control.add((ControlData) d);
        }
      })
      .to(sink);
    assertThat(sink.values()).containsExactly("A", "B", "C", "D", "E", "G", "H", "I");
    assertThat(control).hasSize(4);
  }

  @Test
  public void testWithTransformIncludingControlData() {
    ListSink<String> sink = Sink.list();
    List<ControlData> control = new ArrayList<>();

    Source.from("a", "b", "c", "d", "e", "g", "h", "i")
      .withWindow(Windowing.bySize(2))
      .transform(f -> {
          if (f.isControl()) {
            control.add((ControlData) f);
            return f;
          }
          return f.with(f.payload().toUpperCase());
        }, true)
      .to(sink);
    assertThat(sink.values()).containsExactly("A", "B", "C", "D", "E", "G", "H", "I");
    assertThat(control).hasSize(4);
  }

  @Test
  public void testWithTransformFlow() {
    ListSink<String> sink = Sink.list();
    List<ControlData> control = new ArrayList<>();

    Source.from("a", "b", "c", "d", "e", "g", "h", "i")
      .withWindow(Windowing.bySize(2))
      .transformFlow(f ->
        f.flatMap(data ->
          Flowable.just(data).map(d -> d.with(d.payload().toUpperCase())).repeat(2))
      )
      .onData(d -> {
        System.out.println(d);
        if (ControlData.isControl(d)) {
          control.add((ControlData) d);
        }
      })
      .to(sink);
    assertThat(sink.values()).containsExactly("A", "A", "B", "B", "C", "C", "D", "D",
      "E", "E", "G", "G", "H", "H", "I", "I");
    assertThat(control).hasSize(4);
  }

  @Test
  public void testWithTransformFlowIncludingControlData() {
    ListSink<String> sink = Sink.list();
    List<ControlData> control = new ArrayList<>();

    Source.from("a", "b", "c", "d", "e", "g", "h", "i")
      .withWindow(Windowing.bySize(2))
      .transformFlow(f ->
          f.flatMap(data ->
            Flowable.just(data)
              .flatMap(d -> {
                if (d.isControl()) {
                  return Flowable.just(d).doOnNext(x -> control.add((ControlData) x));
                } else {
                  return Flowable.just(d.with(d.payload().toUpperCase())).repeat(2);
                }
              })
          )
        , true)
      .to(sink);
    assertThat(sink.values()).containsExactly("A", "A", "B", "B", "C", "C", "D", "D",
      "E", "E", "G", "G", "H", "H", "I", "I");
    assertThat(control).hasSize(4);
  }

  @Test
  public void testWithTransformPayloadFlow() {
    ListSink<String> sink = Sink.list();
    List<ControlData> control = new ArrayList<>();

    Source.from("a", "b", "c", "d", "e", "g", "h", "i", "j")
      .withWindow(Windowing.bySize(2))
      .transformPayloadFlow(f ->
        f.flatMap(s ->
          Flowable.just(s).map(String::toUpperCase).repeat(2))
      )
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control.add((ControlData) d);
        }
      })
      .to(sink);
    assertThat(sink.values()).containsExactly("A", "A", "B", "B", "C", "C", "D", "D",
      "E", "E", "G", "G", "H", "H", "I", "I", "J", "J");
    assertThat(control).hasSize(5);

  }

  @Test
  public void testWithZip() {
    ListSink<String> sink = Sink.list();
    List<ControlData> control = new ArrayList<>();

    Source.from("a", "b", "c", "d", "e", "g", "h", "i", "j")
      .withWindow(Windowing.bySize(2))
      .transformPayload(String::toUpperCase)
      .zipWith(Source.fromPayloads(Flowable.range(0, 100)).withWindow(Windowing.bySize(3)))
      .transformPayload(pair -> pair.left() + pair.right())
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control.add((ControlData) d);
        }
      })
      .to(sink);

    assertThat(sink.values()).containsExactly("A0", "B1", "C2", "D3", "E4", "G5", "H6", "I7", "J8");
    assertThat(control).hasSize(5);
  }

  @Test
  public void testWithMultipleZip() {
    ListSink<String> sink = Sink.list();
    List<ControlData> control = new ArrayList<>();

    Source<Integer> number = Source.fromPayloads(Flowable.range(0, 100)).withWindow(Windowing.bySize(3));
    Source<String> greek = Source.from
      ("alpha", "beta", "gamma", "delta", "epsilon", "zeta", "theta", "eta",
        "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi", "rho", "sigma", "tau", "upsilon", "phi", "chi", "psi",
        "omega");

    Source.from("a", "b", "c", "d", "e", "g", "h", "i", "j")
      .withWindow(Windowing.bySize(2))
      .transformPayload(String::toUpperCase)
      .zipWith(number, greek)
      .transformPayload(tuple -> tuple.<String>nth(0) + tuple.nth(1) + " (" + tuple.nth(2) + ")")
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control.add((ControlData) d);
        }
      })
      .to(sink);

    assertThat(sink.values()).containsExactly("A0 (alpha)", "B1 (beta)", "C2 (gamma)", "D3 (delta)", "E4 (epsilon)", "G5 " +
      "(zeta)", "H6 (theta)", "I7 (eta)", "J8 (iota)");
    assertThat(control).hasSize(5);
  }

  @Test
  public void testWithBroadcastNoMerge() {
    ListSink<String> sink1 = Sink.list();
    ListSink<String> sink2 = Sink.list();
    List<ControlData> control1 = new ArrayList<>();
    List<ControlData> control2 = new ArrayList<>();

    Source<Integer> numbers = Source.fromPayloads(Flowable.range(0, 10)).withWindow(Windowing.bySize(3));

    List<DataStream<Integer>> streams = numbers.broadcast(2);
    streams.get(0).transformPayload(i -> Integer.toString(i))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control1.add((ControlData) d);
        }
      })
      .to(sink1);
    streams.get(1).transformPayload(i -> Integer.toString(i))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control2.add((ControlData) d);
        }
      })
      .to(sink2);

    assertThat(sink1.values()).hasSameElementsAs(sink2.values())
      .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    assertThat(control1).hasSameElementsAs(control2);
  }

  @Test
  public void testWithBroadcastWithMerge() {
    ListSink<String> sink1 = Sink.list();
    List<ControlData> control = new ArrayList<>();

    Source<Integer> numbers = Source.fromPayloads(Flowable.range(0, 10)).withWindow(Windowing.bySize(3));

    List<DataStream<Integer>> streams = numbers.broadcast(2);
    streams.get(0)
      .transformPayload(i -> Integer.toString(i))
      .mergeWith(streams.get(1).transformPayload(Integer::toBinaryString))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control.add((ControlData) d);
        }
      })
      .to(sink1);

    assertThat(sink1.values())
      .contains("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "100", "110")
      .hasSize(20);
    assertThat(control).hasSize(4);
  }

  @Test
  public void testWithBranch() {
    ListSink<String> sink1 = Sink.list();
    ListSink<String> sink2 = Sink.list();
    List<ControlData> control1 = new ArrayList<>();
    List<ControlData> control2 = new ArrayList<>();

    Source<Integer> numbers = Source.fromPayloads(Flowable.range(0, 10)).withWindow(Windowing.bySize(3));

    Pair<DataStream<Integer>, DataStream<Integer>> streams = numbers.branch(d -> d.payload() % 2 == 0);
    streams.left().transformPayload(i -> Integer.toString(i))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control1.add((ControlData) d);
        }
      })
      .to(sink1);
    streams.right().transformPayload(i -> Integer.toString(i))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control2.add((ControlData) d);
        }
      })
      .to(sink2);

    assertThat(sink1.values()).containsExactly("0", "2", "4", "6", "8");
    assertThat(sink2.values()).containsExactly("1", "3", "5", "7", "9");
    assertThat(control1).hasSameElementsAs(control2);
  }

  @Test
  public void testWithBranches() {
    ListSink<String> sink1 = Sink.list();
    ListSink<String> sink2 = Sink.list();
    List<ControlData> control1 = new ArrayList<>();
    List<ControlData> control2 = new ArrayList<>();

    Source<Integer> numbers = Source.fromPayloads(Flowable.range(0, 10)).withWindow(Windowing.bySize(3));

    List<DataStream<Integer>> streams = numbers.branch(d -> d.payload() % 2 == 0, d -> d.payload() < 5);
    streams.get(0).transformPayload(i -> Integer.toString(i))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control1.add((ControlData) d);
        }
      })
      .to(sink1);
    streams.get(1).transformPayload(i -> Integer.toString(i))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control2.add((ControlData) d);
        }
      })
      .to(sink2);

    assertThat(sink1.values()).containsExactly("0", "2", "4", "6", "8");
    assertThat(sink2.values()).containsExactly("1", "3");
    assertThat(control1).hasSameElementsAs(control2);
  }

  @Test
  public void testWithBranchOnPayload() {
    ListSink<String> sink1 = Sink.list();
    ListSink<String> sink2 = Sink.list();
    List<ControlData> control1 = new ArrayList<>();
    List<ControlData> control2 = new ArrayList<>();

    Source<Integer> numbers = Source.fromPayloads(Flowable.range(0, 10)).withWindow(Windowing.bySize(3));

    Pair<DataStream<Integer>, DataStream<Integer>> streams = numbers.branchOnPayload(d -> d % 2 == 0);
    streams.left().transformPayload(i -> Integer.toString(i))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control1.add((ControlData) d);
        }
      })
      .to(sink1);
    streams.right().transformPayload(i -> Integer.toString(i))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control2.add((ControlData) d);
        }
      })
      .to(sink2);

    assertThat(sink1.values()).containsExactly("0", "2", "4", "6", "8");
    assertThat(sink2.values()).containsExactly("1", "3", "5", "7", "9");
    assertThat(control1).hasSameElementsAs(control2);
  }

  @Test
  public void testWithBranchesOnPayload() {
    ListSink<String> sink1 = Sink.list();
    ListSink<String> sink2 = Sink.list();
    List<ControlData> control1 = new ArrayList<>();
    List<ControlData> control2 = new ArrayList<>();

    Source<Integer> numbers = Source.fromPayloads(Flowable.range(0, 10)).withWindow(Windowing.bySize(3));

    List<DataStream<Integer>> streams = numbers.branchOnPayload(d -> d % 2 == 0, d -> d < 5);
    streams.get(0).transformPayload(i -> Integer.toString(i))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control1.add((ControlData) d);
        }
      })
      .to(sink1);
    streams.get(1).transformPayload(i -> Integer.toString(i))
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control2.add((ControlData) d);
        }
      })
      .to(sink2);

    assertThat(sink1.values()).containsExactly("0", "2", "4", "6", "8");
    assertThat(sink2.values()).containsExactly("1", "3");
    assertThat(control1).hasSameElementsAs(control2);
  }

  @Test
  public void testOnDataAndOnPayload() {
    List<ControlData> control = new ArrayList<>();
    List<Integer> seenInOnData = new ArrayList<>();
    List<Integer> seenInOnPayload = new ArrayList<>();

    Source<Integer> numbers = Source.fromPayloads(Flowable.range(0, 10)).withWindow(Windowing.bySize(3));
    numbers
      .onPayload(seenInOnPayload::add)
      .onData(d -> {
        if (ControlData.isControl(d)) {
          control.add((ControlData) d);
        } else {
          seenInOnData.add(d.payload());
        }
      })
      .to(Sink.discard());

    assertThat(control).hasSize(4);
    assertThat(seenInOnData).hasSize(10);
    assertThat(seenInOnPayload).hasSize(10);
  }

  @Test
  public void testOnDataAndOnPayloadWithoutControlData() {
    List<Integer> seenInOnData = new ArrayList<>();
    List<Integer> seenInOnPayload = new ArrayList<>();

    Source<Integer> numbers = Source.fromPayloads(Flowable.range(0, 10)).withWindow(Windowing.bySize(3));
    numbers
      .onPayload(seenInOnPayload::add)
      .onData(d ->
        seenInOnData.add(d.payload()), false
      )
      .to(Sink.discard());

    assertThat(seenInOnData).hasSize(10);
    assertThat(seenInOnPayload).hasSize(10);
  }
}
