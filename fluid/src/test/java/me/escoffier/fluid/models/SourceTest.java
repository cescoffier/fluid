package me.escoffier.fluid.models;


import com.google.common.collect.ImmutableList;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import me.escoffier.fluid.impl.ListSink;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

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
  public void testFanInFlows() {
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
  public void testFromPublisherOfMessageCreation() {
    List<Message<Integer>> messages = ImmutableList.of(
      new Message<>(1), new Message<>(2), new Message<>(3)
    );

    Flowable<Message<Integer>> publisher = Flowable.fromIterable(messages);
    ListSink<String> sink = Sink.list();
    Source.from(publisher)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("1", "2", "3");
  }

  @Test
  public void testFromPublisherOfValueCreation() {
    List<Integer> values = ImmutableList.of(
      1, 2, 3, 4, 5
    );

    Flowable<Integer> publisher = Flowable.fromIterable(values);
    ListSink<String> sink = Sink.list();
    Source.fromPayloads(publisher)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("1", "2", "3", "4", "5");
  }

  @Test
  public void testFromIterableOfMessageCreation() {
    List<Message<Integer>> messages = ImmutableList.of(
      new Message<>(1), new Message<>(2), new Message<>(3)
    );

    ListSink<String> sink = Sink.list();
    Source.from(messages)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("1", "2", "3");
  }

  @Test
  public void testFromIterableOfValueCreation() {
    List<Integer> values = ImmutableList.of(
      1, 2, 3, 4, 5
    );

    ListSink<String> sink = Sink.list();
    Source.fromPayloads(values)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("1", "2", "3", "4", "5");
  }

  @Test
  public void testFromASingleOfMessage() {
    Single<Message<Integer>> single = Single.just(new Message<>(25));
    ListSink<String> sink = Sink.list();
    Source.from(single)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("25");
  }

  @Test
  public void testFromASingleOfValue() {
    Single<Integer> single = Single.just(23);
    ListSink<String> sink = Sink.list();
    Source.fromPayload(single)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("23");
  }

  @Test
  public void testFromMaybesOfMessage() {

    Maybe<Message<Integer>> maybe1 = Maybe.just(new Message<>(99));
    Maybe<Message<Integer>> maybe2 = Maybe.empty();

    ListSink<String> sink = Sink.list();
    Source.from(maybe1)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("99");

    sink = Sink.list();
    Source.from(maybe2)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).isEmpty();
  }

  @Test
  public void testFromMaybesOfValue() {

    Maybe<Integer> maybe1 = Maybe.just(98);
    Maybe<Integer> maybe2 = Maybe.empty();

    ListSink<String> sink = Sink.list();
    Source.fromPayload(maybe1)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("98");

    sink = Sink.list();
    Source.fromPayload(maybe2)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).isEmpty();
  }

  @Test
  public void testFromMessagesCreation() {
    ListSink<String> sink = Sink.list();
    Source.from(new Message<>(1), new Message<>(2), new Message<>(3))
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("1", "2", "3");
  }

  @Test
  public void testFromValuesCreation() {
    ListSink<String> sink = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("1", "2", "3", "4", "5");
  }

  @Test
  public void testFromStreamOfMessage() {
    List<Integer> values = ImmutableList.of(
      1, 2, 3, 4, 5
    );

    ListSink<String> sink = Sink.list();
    Source.from(values.stream().map(Message::new))
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("1", "2", "3", "4", "5");
  }

  @Test
  public void testFromStreamOfValues() {
    List<Integer> values = ImmutableList.of(
      1, 2, 3, 4, 5
    );

    ListSink<String> sink = Sink.list();
    Source.fromPayloads(values.stream())
      .mapPayload(i -> Integer.toString(i))
      .to(sink);

    assertThat(sink.values()).containsExactly("1", "2", "3", "4", "5");
  }

  @Test
  public void testCreationWithJustAMessage() {
    Message<String> message = new Message<>("hello");

    ListSink<String> sink = Sink.list();
    Source.just(message)
      .mapPayload(String::toUpperCase)
      .to(sink);

    assertThat(sink.values()).containsExactly("HELLO");
  }

  @Test
  public void testCreationWithJustAValue() {
    ListSink<String> sink = Sink.list();
    Source.just("hello")
      .mapPayload(String::toUpperCase)
      .to(sink);

    assertThat(sink.values()).containsExactly("HELLO");
  }

  @Test
  public void testCreationWithFailed() {
    ListSink<String> sink = Sink.list();
    Source.failed();

    assertThat(sink.values()).isEmpty();
  }

  @Test
  public void testCreationWithFailedWithASpecificException() {
    ListSink<String> sink = Sink.list();
    Source.failed(new Exception("my bad"));

    assertThat(sink.values()).isEmpty();
  }

  @Test
  public void testDefaultGetName() {
    Source<String> s = Source.empty();
    assertThat(s.name()).isNull();
  }





}
