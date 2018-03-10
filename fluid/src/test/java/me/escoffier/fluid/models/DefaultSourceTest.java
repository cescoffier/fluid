package me.escoffier.fluid.models;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.reactivex.Flowable;
import io.reactivex.Single;
import me.escoffier.fluid.impl.ListSink;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Checks the behavior of {@link DefaultSource}.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class DefaultSourceTest {

  @Test
  public void testNamedSource() {
    Source<String> source = Source.from("a", "b", "c")
      .named("my source");

    assertThat(source.name()).isEqualTo("my source");

    assertThat(source.named("foo").name()).isEqualTo("foo");

    assertThat(source.unnamed().name()).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullName() {
    Source.from("a", "b", "c").named(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBlankName() {
    Source.from("a", "b", "c").named(" ");
  }

  @Test
  public void testAttributes() {
    Source<String> source = Source.from("a", "b", "c")
      .named("my source");

    assertThat(source.attr("foo")).isEmpty();

    source = source.withAttribute("foo", "bar").withAttribute("k", "v");
    assertThat(source.attr("foo")).get().isEqualTo("bar");
    assertThat(source.attr("k")).get().isEqualTo("v");

    source = source.withoutAttribute("foo").withoutAttribute("boo");
    assertThat(source.attr("foo")).isEmpty();
    assertThat(source.attr("k")).get().isEqualTo("v");
  }

  @Test
  public void testOrElse() {
    Source<String> source = Source.from("a", "b", "c")
      .named("my source");

    Source<String> empty = Source.<String>empty().named("empty");

    Source<String> another = Source.from("d", "e", "f");

    ListSink<String> sink = Sink.list();
    source.orElse(another).to(sink);
    assertThat(sink.values()).containsExactly("a", "b", "c");

    sink = Sink.list();
    empty.orElse(source).to(sink);
    assertThat(sink.values()).containsExactly("a", "b", "c");

    sink = Sink.list();
    empty.orElse(another).to(sink);
    assertThat(sink.values()).containsExactly("d", "e", "f");

    sink = Sink.list();
    empty.orElse(empty).to(sink);
    assertThat(sink.values()).isEmpty();
  }


  @Test
  public void testMapOnItem() {
    ListSink<Integer> list = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .mapPayload(i -> i + 1)
      .to(list);

    assertThat(list.values()).containsExactly(2, 3, 4, 5, 6);
  }

  @Test
  public void testFilterOnMessage() {
    ListSink<Integer> sink = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .filter(msg -> msg.payload() >= 3)
      .to(sink);
    assertThat(sink.values()).containsExactly(3, 4, 5);
  }

  @Test
  public void testFilterNotOnMessage() {
    ListSink<Integer> sink = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .filterNot(msg -> msg.payload() >= 3)
      .to(sink);
    assertThat(sink.values()).containsExactly(1, 2);
  }

  @Test
  public void testFilterOnPayloads() {
    ListSink<Integer> sink = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .filterPayload(i -> i >= 3)
      .to(sink);
    assertThat(sink.values()).containsExactly(3, 4, 5);
  }

  @Test
  public void testFilterNotOnPayloads() {
    ListSink<Integer> sink = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .filterNotPayload(i -> i >= 3)
      .to(sink);
    assertThat(sink.values()).containsExactly(1, 2);
  }

  @Test
  public void testFlatMap() {
    ListSink<Integer> sink = Sink.list();
    Random random = new Random();
    Source.from(1, 2, 3, 4, 5)
      .flatMap(i -> Flowable.fromArray(i, i).delay(random.nextInt(10), TimeUnit.MILLISECONDS))
      .to(sink);
    await().until(() -> sink.values().size() == 10);
    assertThat(sink.values()).contains(1, 1, 2, 2, 3, 3, 4, 4, 5, 5);
  }

  @Test
  public void testFlatMapWithConcurrency() {
    ListSink<Integer> sink = Sink.list();
    Random random = new Random();
    Source.from(1, 2, 3, 4, 5)
      .flatMap(i -> Flowable.fromArray(i, i).delay(random.nextInt(10), TimeUnit.MILLISECONDS), 1)
      .to(sink);
    await().until(() -> sink.values().size() == 10);
    assertThat(sink.values()).containsExactly(1, 1, 2, 2, 3, 3, 4, 4, 5, 5);
  }

  @Test
  public void testFlatMapOnPayload() {
    ListSink<Integer> sink = Sink.list();
    Random random = new Random();
    Source.from(1, 2, 3, 4, 5)
      .flatMapPayload(i -> Flowable.fromArray(i, i).delay(random.nextInt(10), TimeUnit.MILLISECONDS))
      .to(sink);
    await().until(() -> sink.values().size() == 10);
    assertThat(sink.values()).contains(1, 1, 2, 2, 3, 3, 4, 4, 5, 5);
  }

  @Test
  public void testFlatMapOnPayloadWithConcurrency() {
    ListSink<Integer> sink = Sink.list();
    Random random = new Random();
    Source.from(1, 2, 3, 4, 5)
      .flatMapPayload(i -> Flowable.fromArray(i, i).delay(random.nextInt(10), TimeUnit.MILLISECONDS), 1)
      .to(sink);
    await().until(() -> sink.values().size() == 10);
    assertThat(sink.values()).containsExactly(1, 1, 2, 2, 3, 3, 4, 4, 5, 5);
  }

  @Test
  public void testConcatMap() {
    ListSink<Integer> sink = Sink.list();
    Random random = new Random();
    Source.from(1, 2, 3, 4, 5)
      .concatMap(i -> Flowable.fromArray(i, i).delay(random.nextInt(10), TimeUnit.MILLISECONDS))
      .to(sink);
    await().until(() -> sink.values().size() == 10);
    assertThat(sink.values()).containsExactly(1, 1, 2, 2, 3, 3, 4, 4, 5, 5);
  }

  @Test
  public void testConcatMapOnPayload() {
    ListSink<Integer> sink = Sink.list();
    Random random = new Random();
    Source.from(1, 2, 3, 4, 5)
      .concatMapPayload(i -> Flowable.fromArray(i, i).delay(random.nextInt(10), TimeUnit.MILLISECONDS))
      .to(sink);
    await().until(() -> sink.values().size() == 10);
    assertThat(sink.values()).containsExactly(1, 1, 2, 2, 3, 3, 4, 4, 5, 5);
  }

  @Test
  public void testCompose() {
    ListSink<Integer> list = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .composeFlowable(flow -> flow.map(Message::payload).map(i -> i + 1).map(Message::new))
      .to(list);

    assertThat(list.values()).containsExactly(2, 3, 4, 5, 6);
  }

  @Test
  public void testTransformer() {
    ListSink<Integer> list = Sink.list();
    Source.from(1, 2, 3, 4, 5)
      .composePayloadFlowable(flow -> flow.map(i -> i + 1))
      .to(list);
    assertThat(list.values()).containsExactly(2, 3, 4, 5, 6);
  }

  @Test
  public void testThatWeCanRetrieveTheFlowable() {
    List<Integer> list = Source.from(1, 2, 3, 4, 5)
      .asFlowable()
      .map(Message::payload)
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
    s1.mergeWith(s2).to(list);

    assertThat(list.values()).containsExactly("a", "b", "c", "d", "e", "f");

    Random random = new Random();
    ListSink<String> list2 = Sink.list();
    s1.composeFlowable(flow ->
      flow.delay(s ->
        Single.just(s)
          .delay(random.nextInt(10), TimeUnit.MILLISECONDS)
          .toFlowable()))
      .mergeWith(s2, s3)
      .to(list2);

    await().atMost(1, TimeUnit.MINUTES).until(() -> list2.values().size() == 8);
    assertThat(list2.values()).contains("a", "b", "c", "d", "e", "f", "g", "h");
  }

  @Test
  public void testZipWithAnotherStream() {
    Source<String> s1 = Source.from("a", "b", "c");
    Source<String> s2 = Source.from("d", "e", "f");
    Source<String> s3 = Source.from("g", "h", "i");

    ListSink<String> list = Sink.list();
    s1.composePayloadFlowable(s -> s.map(String::toUpperCase))
      .zipWith(s2)
      .mapPayload(pair -> pair.left() + "x" + pair.right())
      .to(list);

    assertThat(list.values()).containsExactly("Axd", "Bxe", "Cxf");

    list = Sink.list();
    s1.composePayloadFlowable(s -> s.map(String::toUpperCase))
      .zipWith(s2, s3)
      .mapPayload(tuple -> tuple.nth(0) + "x" + tuple.nth(1) + "x" + tuple.nth(2))
      .to(list);

    assertThat(list.values()).containsExactly("Axdxg", "Bxexh", "Cxfxi");
  }

  @Test
  public void testDataTransformation() {
    ListSink<Integer> sink = new ListSink<>();
    Source.from(Flowable.range(0, 10).map(i -> random()))
      .map(data -> data.with((int) (data.payload() * 100)))
      .to(sink);
    assertThat(sink.values()).hasSize(10);
    assertThat(sink.data()).hasSize(10);

    for (Message<Integer> d : sink.data()) {
      assertThat(d.<Long>get("X-Timestamp")).isNotNull().isGreaterThanOrEqualTo(0);
      assertThat(d.<Boolean>get("Random")).isTrue();
      assertThat(d.payload()).isGreaterThanOrEqualTo(0).isLessThan(100);
    }
  }

  @Test
  public void testScanWithMessages() {
    List<Message<Integer>> messages = ImmutableList.of(new Message<>(1), new Message<>(2), new Message<>(3), new
      Message<>(4));

    ListSink<Integer> sink = Sink.list();
    Source.from(messages)
      .scan(new Message<>(0), (m1, m2) -> new Message<>(m1.payload() + m2.payload()))
      .to(sink);

    assertThat(sink.values()).containsExactly(0, 1, 3, 6, 10);
  }

  @Test
  public void testScanWithPayloads() {
    List<Message<Integer>> messages = ImmutableList.of(new Message<>(1), new Message<>(2), new Message<>(3), new
      Message<>(4));

    ListSink<Integer> sink = Sink.list();
    Source.from(messages)
      .scanPayloads(0, (a, b) -> a + b)
      .to(sink);

    assertThat(sink.values()).containsExactly(0, 1, 3, 6, 10);
  }

  @Test
  public void testGroupBy() {
    String text = "In 1815, M. Charles–Francois-Bienvenu Myriel was Bishop of D He was an old man of about seventy-five " +
      "years of age; he had occupied the see of D since 1806. Although this detail has no connection whatever with the " +
      "real substance of what we are about to relate, it will not be superfluous, if merely for the sake of exactness in all" +
      " points, to mention here the various rumors and remarks which had been in circulation about him from the very moment " +
      "when he arrived in the diocese. True or false, that which is said of men often occupies as important a place in their" +
      " lives, and above all in their destinies, as that which they do. M. Myriel was the son of a councillor of the " +
      "Parliament of Aix; hence he belonged to the nobility of the bar. It was said that his father, destining him to be the" +
      " heir of his own post, had married him at a very early age, eighteen or twenty, in accordance with a custom which is " +
      "rather widely prevalent in parliamentary families. In spite of this marriage, however, it was said that Charles Myriel" +
      " created a great deal of talk. He was well formed, though rather short in stature, elegant, graceful, intelligent; the" +
      " whole of the first portion of his life had been devoted to the world and to gallantry.";

    Publisher<GroupedDataStream<Character, String>> publisher = Source.from(
      text.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+"))
      .mapPayload(String::toLowerCase)
      .groupBy(msg -> msg.payload().charAt(0));

    Multimap<Character, String> wordsByFirstLetter = ArrayListMultimap.create();

    Flowable.fromPublisher(publisher)
      .doOnNext(stream -> Flowable.fromPublisher(stream).subscribe(word -> wordsByFirstLetter.put(stream.key(), word.payload())))
      .subscribe();

    assertThat(wordsByFirstLetter.get('a')).hasSize(25).contains("aix");
    assertThat(wordsByFirstLetter.get('y')).hasSize(1).contains("years");
    assertThat(wordsByFirstLetter.get('w')).hasSize(21).contains("widely");
    assertThat(wordsByFirstLetter.get('j')).isEmpty();
  }

  @Test
  public void testBranch() {
    Pair<Source<Integer>, Source<Integer>> branches = Source.from(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      .branch(i -> i.payload() % 3 == 0);

    ListSink<String> left = Sink.list();
    ListSink<String> right = Sink.list();
    branches.left().mapPayload(i -> Integer.toString(i)).to(left);
    branches.right().mapPayload(i -> Integer.toString(i)).to(right);

    assertThat(left.values()).containsExactly("3", "6", "9");
    assertThat(right.values()).containsExactly("1", "2", "4", "5", "7", "8", "10");
  }

  @Test
  public void testBranchOnPayload() {
    Pair<Source<Integer>, Source<Integer>> branches = Source.from(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      .branchOnPayload(i -> i % 3 == 0);

    ListSink<String> left = Sink.list();
    ListSink<String> right = Sink.list();
    branches.left().mapPayload(i -> Integer.toString(i)).to(left);
    branches.right().mapPayload(i -> Integer.toString(i)).to(right);

    assertThat(left.values()).containsExactly("3", "6", "9");
    assertThat(right.values()).containsExactly("1", "2", "4", "5", "7", "8", "10");
  }


  private static Message<Double> random() {
    return new Message<>(Math.random()).with("X-Timestamp", System.currentTimeMillis()).with("Random", true);
  }


}
