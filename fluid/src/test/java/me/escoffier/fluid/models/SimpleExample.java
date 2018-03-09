package me.escoffier.fluid.models;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import me.escoffier.fluid.impl.HeadSink;
import me.escoffier.fluid.impl.ListSink;
import me.escoffier.fluid.impl.ScanSink;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SimpleExample {


  private Vertx vertx;

  @Before
  public void setup() {
    vertx = Vertx.vertx();
  }

  @After
  public void teardown() {
    vertx.close();
  }

  @Test
  public void testWithRange() {
    ListSink<Integer> sink = new ListSink<>();
    Source.from(Flowable.range(0, 10).map(Message::new))
      .mapPayload(i -> {
        System.out.println("Item: " + i);
        return i;
      })
      .to(sink);

    await().until(() -> sink.values().size() == 10);
    assertThat(sink.values().size()).isEqualTo(10);
  }

  @Test
  public void testWithRange2() {
    List<Message<Integer>> list = new ArrayList<>();
    Source.from(Flowable.range(0, 10).map(Message::new))
      .to(Sink.forEach(list::add));
    assertThat(list).hasSize(10);
  }

  @Test
  public void testWithFactorial() throws IOException {
    String path = "target/test-classes/factorial.txt";
    FileSink sink = new FileSink(vertx, path);
    getFactorialFlow()
      .mapPayload(i -> i.toString() + "\n")
      .to(sink);

    await().until(() -> FileUtils.readLines(new File(path), "UTF-8").size() >= 10);
    assertThat(FileUtils.readLines(new File(path), "UTF-8")).contains("3628800");
  }

  @Test
  public void testQuotes() {
    ListSink<String> cache = new ListSink<>();
    List<Quote> quotes = new ArrayList<>();
    quotes.add(new Quote("Attitude is everything", "Diane Von Furstenberg"));
    quotes.add(new Quote("Life is short, heels shouldn't be", "Brian Atwood"));
    quotes.add(new Quote("Red is the color for fall", "Piera Gelardi"));
    quotes.add(new Quote("Rhinestones make everything better", "Piera Gelardi"));
    quotes.add(new Quote("Design is so simple, that's why it's so complicated", "Paul Rand"));

    Flowable<Message<String>> stream = Source.fromPayloads(quotes.stream())
      .mapPayload(q -> q.author)
      .asFlowable()
      .map(Message::payload)
      .compose(Flowable::distinct)
      .map(d -> new Message<>(d.toUpperCase()));

    Source.from(stream)
      .to(cache);

    await().until(() -> cache.values().size() == 4);
    assertThat(cache.values()).contains("PAUL RAND", "PIERA GELARDI");


  }

  private class Quote {
    final String quote;
    final String author;

    Quote(String quote, String author) {
      this.author = author;
      this.quote = quote;
    }
  }

  private Sink<BigInteger> toLineInFile() {
    String path = "target/test-classes/factorial-2.txt";
    FileSink sink = new FileSink(vertx, path);
    return data ->
      Single.just(data)
        .map(d -> d.with(d.payload().toString() + "\n"))
        .flatMapCompletable(sink::dispatch);
  }

  @Test
  public void testWithFactorialUsingBuiltSink() throws IOException {
    String path = "target/test-classes/factorial-2.txt";
    getFactorialFlow()
      .to(toLineInFile());

    await().until(() -> FileUtils.readLines(new File(path), "UTF-8").size() >= 10);
    assertThat(FileUtils.readLines(new File(path), "UTF-8")).contains("3628800");
  }

  private Source<BigInteger> getFactorialFlow() {
    return Source.fromPayloads(Flowable.range(1, 10))
      .scanPayloads(BigInteger.ONE,
        (acc, next) -> acc.multiply(BigInteger.valueOf(next)));
  }

  @Test
  public void testTimeBasedManipulation() {
    ListSink<String> cache = new ListSink<>();
    getFactorialFlow()
      .compose(publisher -> {
        Flowable<Message<BigInteger>> flowable = Flowable.fromPublisher(publisher);
        return flowable.zipWith(Flowable.range(0, 99),
          (num, idx) -> String.format("%d! = %s", idx, num))
          .delay(1, TimeUnit.SECONDS)
          .map(Message::new);
        })
      .to(cache);

    await().until(() -> cache.values().size() >= 10);
    assertThat(cache.values().size()).isGreaterThanOrEqualTo(10);
  }

  @Test
  public void testComplexShaping() {
    Function<Publisher<Message<Quote>>, Publisher<Message<String>>> toAuthor =
      flow -> Flowable.fromPublisher(flow).map(q -> q.with(q.payload().author));

    Function<Publisher<Message<Quote>>, Publisher<Message<String>>> toWords =
      flow -> Flowable.fromPublisher(flow)
        .map(Message::payload)
        .concatMap(quote -> Flowable.fromArray(quote.quote.split(" ")))
        .map(Message::new);

    ListSink<String> authors = new ListSink<>();
    ListSink<String> words = new ListSink<>();

    List<Quote> quotes = new ArrayList<>();
    quotes.add(new Quote("Attitude is everything", "Diane Von Furstenberg"));
    quotes.add(new Quote("Life is short, heels shouldn't be", "Brian Atwood"));
    quotes.add(new Quote("Red is the color for fall", "Piera Gelardi"));
    quotes.add(new Quote("Rhinestones make everything better", "Piera Gelardi"));
    quotes.add(new Quote("Design is so simple, that's why it's so complicated", "Paul Rand"));

    List<Source<Quote>> broadcast = Source.from(quotes.stream().map(Message::new)).broadcast(2);

    broadcast.get(0)
      .compose(toAuthor)
      .compose(publisher -> Flowable.fromPublisher(publisher).map(Message::payload).distinct().map(Message::new))
      .to(authors);

    broadcast.get(1)
      .compose(toWords)
      .compose(publisher -> Flowable.fromPublisher(publisher).distinct())
      .to(words);

    await().until(() -> authors.values().size() == 4);
    assertThat(authors.values()).hasSize(4);
    assertThat(words.values()).isNotEmpty();
  }

  @Test
  public void testMerge() {
    Flowable<String> f1 = Flowable.fromArray("a", "b", "c")
      .delay(10, TimeUnit.MILLISECONDS);

    List<Source<String>> broadcast = Source.fromPayloads(f1).broadcast(2);

    Source<String> stream1 = broadcast.get(0)
      .mapPayload(String::toUpperCase);
    Source<String> stream2 = broadcast.get(1)
      .mapPayload(s -> "FOO");

    ListSink<String> cache = new ListSink<>();
    stream1.mergeWith(stream2).to(cache);

    await().until(() -> cache.values().size() == 6);
    assertThat(cache.values()).contains("A", "B", "C").contains("FOO");
  }


  @Test
  public void testZip() {
    Flowable<String> f1 = Flowable.fromArray("a", "b", "c");

    Flowable<String> f2 = Flowable.fromArray("1", "2", "3");

    ListSink<String> cache = new ListSink<>();
    Source.fromPayloads(f1).mapPayload(String::toUpperCase).zipWith(Source.fromPayloads(f2))
      .mapPayload(pair -> pair.left() + ":" + pair.right() + "\n")
      .to(cache);

    await().until(() -> cache.values().size() == 3);
    assertThat(cache.values()).containsExactly("A:1\n", "B:2\n", "C:3\n");
  }

  @Test
  public void testFold() {
    ScanSink<Integer, Integer> sink = Sink.fold(0, (i, v) -> i + v);
    Source.fromPayloads(Flowable.range(0, 3))
      .mapPayload(i -> ++i)
      .to(sink);

    assertThat(sink.value()).isEqualTo(6);
  }

  @Test
  public void testHead() {
    HeadSink<Integer> sink = Sink.head();
    Source.fromPayloads(Flowable.range(0, 3))
      .mapPayload(i -> ++i)
      .to(sink);

    assertThat(sink.value()).isEqualTo(1);
  }


}
