package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    CacheSink<Integer> sink = new CacheSink<>();
    Source.from(Flowable.range(0, 10).map(Data::new))
      .transformPayload(i -> {
        System.out.println("Item: " + i);
        return i;
      })
      .to(sink);

    await().until(() -> sink.buffer.size() == 10);
  }

  @Test
  public void testWithRange2() {
    Source.from(Flowable.range(0, 10).map(Data::new))
      .to(Sink.forEach(System.out::println));
  }

  @Test
  public void testWithFactorial() throws IOException {
    String path = "target/test-classes/factorial.txt";
    FileSink sink = new FileSink(vertx, path);
    getFactorialFlow()
      .transformPayload(i -> i.toString() + "\n")
      .to(sink);

    await().until(() -> FileUtils.readLines(new File(path), "UTF-8").size() >= 10);
    assertThat(FileUtils.readLines(new File(path), "UTF-8")).contains("3628800");
  }

  @Test
  public void testQuotes() {
    CacheSink<String> cache = new CacheSink<>();
    List<Quote> quotes = new ArrayList<>();
    quotes.add(new Quote("Attitude is everything", "Diane Von Furstenberg"));
    quotes.add(new Quote("Life is short, heels shouldn't be", "Brian Atwood"));
    quotes.add(new Quote("Red is the color for fall", "Piera Gelardi"));
    quotes.add(new Quote("Rhinestones make everything better", "Piera Gelardi"));
    quotes.add(new Quote("Design is so simple, that's why it's so complicated", "Paul Rand"));

    Source.fromPayloads(quotes.stream())
      .transformPayload(q -> q.author)
      .transformPayloadFlow(Flowable::distinct)
      .transformPayload(String::toUpperCase)
      .to(cache);

    await().until(() -> cache.cache().size() == 4);
    assertThat(cache.cache()).contains("PAUL RAND", "PIERA GELARDI");


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

  private DataStream<BigInteger> getFactorialFlow() {
    return Source.fromPayloads(Flowable.range(1, 10))
      .transformPayloadFlow(flow -> flow.scan(BigInteger.ONE,
        (acc, next) -> acc.multiply(BigInteger.valueOf(next))));
  }

  @Test
  public void testTimeBasedManipulation() {
    CacheSink<String> cache = new CacheSink<>();
    getFactorialFlow()
      .transformPayloadFlow(flow ->
        flow.zipWith(Flowable.range(0, 99),
          (num, idx) -> String.format("%d! = %s", idx, num))
          .delay(1, TimeUnit.SECONDS))
      .to(cache);

    await().until(() -> cache.buffer.size() >= 10);
  }

  @Test
  public void testComplexShaping() {
    Function<Flowable<Quote>, Flowable<String>> toAuthor = flow -> flow.map(q -> q.author);
    Function<Flowable<Quote>, Flowable<String>> toWords = flow -> flow
      .concatMap(q -> Flowable.fromArray(q.quote.split(" ")));
    CacheSink<String> authors = new CacheSink<>();
    CacheSink<String> words = new CacheSink<>();

    List<Quote> quotes = new ArrayList<>();
    quotes.add(new Quote("Attitude is everything", "Diane Von Furstenberg"));
    quotes.add(new Quote("Life is short, heels shouldn't be", "Brian Atwood"));
    quotes.add(new Quote("Red is the color for fall", "Piera Gelardi"));
    quotes.add(new Quote("Rhinestones make everything better", "Piera Gelardi"));
    quotes.add(new Quote("Design is so simple, that's why it's so complicated", "Paul Rand"));

    DataStream<String> s1 = DataStream.of(Quote.class)
      .transformPayloadFlow(toAuthor)
      .transformPayloadFlow(Flowable::distinct);
    DataStream<String> s2 = DataStream.of(Quote.class)
      .transformPayloadFlow(toWords)
      .transformPayloadFlow(Flowable::distinct);

    // TODO Here we can't check the type connection, we may need to adapt s1 and s2 as sink.
    Source.from(quotes.stream().map(Data::new))
      .broadcastTo(s1, s2);

    s1.to(authors);
    s2.to(words);

    await().until(() -> authors.cache().size() == 4);
    System.out.println(authors.cache());
    System.out.println(words.cache());
  }

  @Test
  public void testMerge() {
    Flowable<String> f1 = Flowable.fromArray("a", "b", "c")
      .delay(10, TimeUnit.MILLISECONDS);

    DataStream<String> s1 = DataStream.of(String.class)
      .transformPayload(String::toUpperCase);
    DataStream<String> s2 = DataStream.of(String.class)
      .transformPayload(s -> "FOO");
    Source.fromPayloads(f1).broadcastTo(s1, s2);

    CacheSink<String> cache = new CacheSink<>();
    s1.mergeWith(new DataStream[]{s2}).to(cache);

    await().until(() -> cache.cache().size() == 6);
    assertThat(cache.cache()).contains("A", "B", "C").contains("FOO");
  }

  @Test
  public void testConcat() {
    Flowable<String> f1 = Flowable.fromArray("a", "b", "c")
      .delay(10, TimeUnit.MILLISECONDS);

    DataStream<String> s1 = DataStream.of(String.class)
      .transformPayload(String::toUpperCase);
    DataStream<String> s2 = DataStream.of(String.class)
      .transformPayload(s -> "FOO");
    Source.fromPayloads(f1).broadcastTo(s1, s2);

    CacheSink<String> cache = new CacheSink<>();
    s1.concatWith(new DataStream[]{s2}).to(cache);

    await().until(() -> cache.cache().size() == 6);
    assertThat(cache.cache()).containsExactly("A", "B", "C", "FOO", "FOO", "FOO");
  }

  @Test
  public void testZip() {
    Flowable<String> f1 = Flowable.fromArray("a", "b", "c");

    Flowable<String> f2 = Flowable.fromArray("1", "2", "3");

    CacheSink<String> cache = new CacheSink<>();
    Source.fromPayloads(f1).transformPayload(String::toUpperCase).zipWith(Source.fromPayloads(f2))
      .transformPayload(pair -> pair.left() + ":" + pair.right() + "\n")
      .to(cache);

    await().until(() -> cache.cache().size() == 3);
    assertThat(cache.cache()).containsExactly("A:1\n", "B:2\n", "C:3\n");
  }

  @Test
  public void testFold() {
    ScanSink<Integer, Integer> sink = Sink.fold(0, (i, v) -> i + v);
    Source.fromPayloads(Flowable.range(0, 3))
      .transformPayload(i -> ++i)
      .to(sink);

    await().until(() -> sink.value() == 6);
  }

  @Test
  public void testHead() {
    HeadSink<Integer> sink = Sink.head();
    Source.fromPayloads(Flowable.range(0, 3))
      .transformPayload(i -> ++i)
      .to(sink);

    await().until(() -> sink.value() == 1);
  }


}
