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
      .transformPayload(tuple -> tuple.<String>nth(0) + tuple.nth(1) + " (" + tuple.nth(2) +")")
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
}
