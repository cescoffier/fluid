package me.escoffier.fluid.models;

import io.reactivex.Flowable;
import io.reactivex.Single;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.util.Strings;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static me.escoffier.fluid.models.Pair.pair;

/**
 * The default source implementation.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class DefaultSource<T> implements Source<T> {

  public static final String FUNCTION_CANNOT_BE_NULL_MESSAGE = "The `mapper` function cannot be `null`";
  public static final String FILTER_CANNOT_BE_NULL_MESSAGE = "The `filter` function cannot be `null`";
  private final Publisher<Message<T>> flow;

  private final String name;

  private final Map<String, Object> attributes;

  public DefaultSource(Publisher<Message<T>> items, String name, Map<String, Object> attr) {
    Objects.requireNonNull(items, "The given `items` cannot be `null`");
    this.flow = items;
    this.name = name;
    if (attr != null) {
      this.attributes = Collections.unmodifiableMap(new HashMap<>(attr));
    } else {
      this.attributes = Collections.emptyMap();
    }
  }

  @Override
  public Source<T> named(String name) {
    if (Strings.isBlank(name)) {
      throw new IllegalArgumentException("The name cannot be `null` or blank");
    }
    return new DefaultSource<>(flow, name, attributes);
  }

  @Override
  public Source<T> unnamed() {
    return new DefaultSource<>(flow, null, attributes);
  }

  @Override
  public Source<T> withAttribute(String key, Object value) {
    Map<String, Object> attr = new HashMap<>(attributes);
    attr.put(Objects.requireNonNull(key, "The key must not be `null`"),
      Objects.requireNonNull(value, "The value must not be `null`"));
    return new DefaultSource<>(flow, name, attr);
  }

  @Override
  public Source<T> withoutAttribute(String key) {
    Objects.requireNonNull(key, "name must not be `null`");
    Map<String, Object> attr = new HashMap<>(attributes);
    attr.remove(key);
    return new DefaultSource<>(flow, name, attr);
  }

  @Override
  public void subscribe(Subscriber<? super Message<T>> s) {
    flow.subscribe(s);
  }

  @Override
  public Source<T> orElse(Source<T> alt) {
    Objects.requireNonNull(alt, "The alternative source must not be `null`");
    return new DefaultSource<>(Flowable.fromPublisher(flow).switchIfEmpty(alt), name, attributes);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Optional<T> attr(String key) {
    return Optional.ofNullable((T) attributes.get(Objects.requireNonNull(key, "The key must not be `null`")));
  }

  // TODO dispatchOn to select on which thread pool the source emit the data

  @Override
  public <X> Source<X> map(Function<Message<T>, Message<X>> mapper) {
    Objects.requireNonNull(mapper, FUNCTION_CANNOT_BE_NULL_MESSAGE);
    return new DefaultSource<>(Flowable.fromPublisher(flow).map(mapper::apply), name, attributes);
  }

  @Override
  public <X> Source<X> mapPayload(Function<T, X> mapper) {
    Objects.requireNonNull(mapper, FUNCTION_CANNOT_BE_NULL_MESSAGE);
    Flowable<Message<X>> flowable = Flowable.fromPublisher(flow).map(d -> d.with(mapper.apply(d.payload())));
    return new DefaultSource<>(flowable, name, attributes);
  }

  @Override
  public Source<T> filter(Predicate<Message<T>> filter) {
    Objects.requireNonNull(filter, FILTER_CANNOT_BE_NULL_MESSAGE);
    return new DefaultSource<>(Flowable.fromPublisher(flow).filter(filter::test), name, attributes);
  }

  @Override
  public Source<T> filterPayload(Predicate<T> filter) {
    Objects.requireNonNull(filter, FILTER_CANNOT_BE_NULL_MESSAGE);
    Flowable<Message<T>> flowable = Flowable.fromPublisher(flow).filter(d -> filter.test(d.payload()));
    return new DefaultSource<>(flowable, name, attributes);
  }

  @Override
  public Source<T> filterNot(Predicate<Message<T>> filter) {
    Objects.requireNonNull(filter, FILTER_CANNOT_BE_NULL_MESSAGE);
    return new DefaultSource<>(Flowable.fromPublisher(flow).filter(d -> !filter.test(d)), name, attributes);
  }

  @Override
  public Source<T> filterNotPayload(Predicate<T> filter) {
    Objects.requireNonNull(filter, FILTER_CANNOT_BE_NULL_MESSAGE);
    Flowable<Message<T>> flowable = Flowable.fromPublisher(flow).filter(d -> !filter.test(d.payload()));
    return new DefaultSource<>(flowable, name, attributes);
  }

  @Override
  public <X> Source<X> flatMap(Function<Message<T>, Publisher<Message<X>>> mapper) {
    Objects.requireNonNull(mapper, FUNCTION_CANNOT_BE_NULL_MESSAGE);
    return new DefaultSource<>(Flowable.fromPublisher(flow).flatMap(mapper::apply), name, attributes);
  }

  @Override
  public <X> Source<X> concatMap(Function<Message<T>, Publisher<Message<X>>> mapper) {
    Objects.requireNonNull(mapper, FUNCTION_CANNOT_BE_NULL_MESSAGE);
    return new DefaultSource<>(Flowable.fromPublisher(flow).concatMap(mapper::apply), name, attributes);
  }

  @Override
  public <X> Source<X> flatMap(Function<Message<T>, Publisher<Message<X>>> mapper, int maxConcurrency) {
    Objects.requireNonNull(mapper, FUNCTION_CANNOT_BE_NULL_MESSAGE);
    if (maxConcurrency < 1) {
      throw new IllegalArgumentException("The `maxConcurrency` cannot be less than 1");
    }
    return new DefaultSource<>(Flowable.fromPublisher(flow).flatMap(mapper::apply, maxConcurrency), name, attributes);
  }

  @Override
  public <X> Source<X> flatMapPayload(Function<T, Publisher<X>> mapper) {
    Objects.requireNonNull(mapper, FUNCTION_CANNOT_BE_NULL_MESSAGE);

    Flowable<Message<X>> flowable = Flowable.fromPublisher(flow)
      .flatMap(data -> {
        Publisher<X> publisher = mapper.apply(data.payload());
        return Flowable.fromPublisher(publisher).map(data::with);
      });

    return new DefaultSource<>(flowable, name, attributes);
  }

  @Override
  public <X> Source<X> concatMapPayload(Function<T, Publisher<X>> mapper) {
    Objects.requireNonNull(mapper, FUNCTION_CANNOT_BE_NULL_MESSAGE);

    Flowable<Message<X>> flowable = Flowable.fromPublisher(flow)
      .concatMap(data -> {
        Publisher<X> publisher = mapper.apply(data.payload());
        return Flowable.fromPublisher(publisher).map(data::with);
      });

    return new DefaultSource<>(flowable, name, attributes);
  }

  @Override
  public <X> Source<X> flatMapPayload(Function<T, Publisher<X>> mapper, int maxConcurrency) {
    Objects.requireNonNull(mapper, FUNCTION_CANNOT_BE_NULL_MESSAGE);

    Flowable<Message<X>> flowable = Flowable.fromPublisher(flow)
      .flatMap(data -> {
        Publisher<X> publisher = mapper.apply(data.payload());
        return Flowable.fromPublisher(publisher).map(data::with);
      }, maxConcurrency);

    return new DefaultSource<>(flowable, name, attributes);
  }

  @Override
  public <X> Source<X> scan(Message<X> zero, BiFunction<Message<X>, Message<T>, Message<X>> function) {
    Objects.requireNonNull(function, "The `function` cannot be `null`");
    Objects.requireNonNull(zero, "The `zero` item (seed) cannot be `null`");
    Flowable<Message<X>> reduced = Flowable.fromPublisher(flow).scan(zero, function::apply);
    return new DefaultSource<>(reduced, name, attributes);
  }

  @Override
  public <X> Source<X> scanPayloads(X zero, BiFunction<X, T, X> function) {
    Objects.requireNonNull(function, "The `function` cannot be `null`");
    Flowable<Message<X>> reduced = Flowable.fromPublisher(flow)
      .map(Message::payload)
      .scan(zero, function::apply)
      .map(Message::new); // TODO We are loosing the headers
    return new DefaultSource<>(reduced, name, attributes);
  }

  @Override
  public <K> Publisher<GroupedDataStream<K, T>> groupBy(Function<Message<T>, K> keySupplier) {
    Objects.requireNonNull(keySupplier, "The function computing the key must not be `null`");
    return Flowable.fromPublisher(flow)
      .groupBy(keySupplier::apply)
      .flatMapSingle(gf -> {
        GroupedDataStream<K, T> stream = new GroupedDataStream<>(gf.getKey(), gf);
        return Single.just(stream);
      });
  }

  // TODO Windowing here

  @Override
  public Source<T> log(String loggerName) {
    Flowable<Message<T>> flowable = Flowable.fromPublisher(flow)
      .doOnNext(d -> {
        if (name != null) {
          LogManager.getLogger(loggerName).info("Received data on source '" + name + "': " + d.toString());
        } else {
          LogManager.getLogger(loggerName).info("Received data on unnamed source: " + d.toString());
        }
      })
      .doOnComplete(() -> {
        if (name != null) {
          LogManager.getLogger(loggerName).info("End of emissions on source '" + name + "'");
        } else {
          LogManager.getLogger(loggerName).info("End of emissions on unnamed source");
        }
      })
      .doOnError(error -> {
        if (name != null) {
          LogManager.getLogger(loggerName).error("Error emitted on source '" + name + "'", error);
        } else {
          LogManager.getLogger(loggerName).error("Error emitted on unnamed source", error);
        }
      });
    return new DefaultSource<>(flowable, name, attributes);
  }

  @Override
  public List<Source<T>> broadcast(int numberOfBranches) {
    if (numberOfBranches <= 1) {
      throw new IllegalArgumentException("The number of branch must be at least 2");
    }

    List<Source<T>> streams = new ArrayList<>(numberOfBranches);
    Flowable<Message<T>> publish = Flowable.fromPublisher(flow).publish().autoConnect(numberOfBranches);

    for (int i = 0; i < numberOfBranches; i++) {
      Source<T> stream = new DefaultSource<>(publish, name, attributes);
      streams.add(stream);
    }

    return streams;
  }

  @Override
  public Map<String, Source<T>> broadcast(String... names) {
    if (names == null || names.length <= 1) {
      throw new IllegalArgumentException("The number of branch must be at least 2");
    }

    Map<String, Source<T>> streams = new LinkedHashMap<>();
    Flowable<Message<T>> publish = Flowable.fromPublisher(flow).publish().autoConnect(names.length);

    for (String n : names) {
      if (Strings.isBlank(n)) {
        throw new IllegalArgumentException("Illegal name for source. The name must not be `null` or blank");
      }
      Source<T> stream = new DefaultSource<>(publish, n, attributes);
      streams.put(n, stream);
    }

    return streams;
  }

  @Override
  public Pair<Source<T>, Source<T>> branch(Predicate<Message<T>> condition) {
    List<Source<T>> sources = broadcast(2);
    Source<T> left = sources.get(0).filter(condition);
    Source<T> right = sources.get(0).filterNot(condition);
    return pair(left, right);
  }

  @Override
  public Pair<Source<T>, Source<T>> branchOnPayload(Predicate<T> condition) {
    List<Source<T>> sources = broadcast(2);
    Source<T> left = sources.get(0).filterPayload(condition);
    Source<T> right = sources.get(0).filterNotPayload(condition);
    return pair(left, right);
  }

  // TODO Multi-branch

  @Override
  public Sink<T> to(Sink<T> sink) {
    Objects.requireNonNull(sink, "The sink must not be `null`");
    Flowable.fromPublisher(flow)
      .flatMapCompletable(sink::dispatch)
      .doOnError(Throwable::printStackTrace) // TODO error reporting
      .subscribe();
    return sink;
  }

  @Override
  public Flowable<Message<T>> asFlowable() {
    return Flowable.fromPublisher(this);
  }

  @Override
  public <O> Source<Pair<T, O>> zipWith(Publisher<Message<O>> source) {
    return new DefaultSource<>(asFlowable().zipWith(source, (a, b) -> a.with(Pair.pair(a.payload(), b.payload()))),
      name, attributes);
  }

  @Override
  public Source<Tuple> zipWith(Source... sources) {
    Publisher[] publishers = Arrays.stream(sources).map((Function<Source, Flowable>) Source::asFlowable).toArray(Publisher[]::new);
    return zipWith(publishers);
  }

  //  @Override
  public Source<Tuple> zipWith(Publisher<Message>... sources) {

    List<Flowable<Message>> toBeZipped = new ArrayList<>();
    toBeZipped.add(Flowable.fromPublisher(this));
    for (Publisher<Message> p : sources) {
      toBeZipped.add(Flowable.fromPublisher(p));
    }

    Flowable<Message<Tuple>> stream = Flowable.zip(toBeZipped, objects -> {
      List<Object> payloads = new ArrayList<>();
      Message<?> first = null;
      for (Object o : objects) {
        if (!(o instanceof Message)) {
          throw new IllegalArgumentException("Invalid incoming item - " + Message.class.getName() + " expected, received " +
            o.getClass().getName());
        } else {
          if (first == null) {
            first = ((Message) o);
          }
          payloads.add(((Message) o).payload());
        }
      }
      if (first == null) {
        throw new IllegalStateException("Invalid set of stream");
      }
      return first.with(Tuple.tuple(payloads.toArray(new Object[payloads.size()])));
    });

    return new DefaultSource<>(stream, name, attributes);
  }

  @Override
  public Source<T> mergeWith(Publisher<Message<T>> source) {
    return new DefaultSource<>(asFlowable().mergeWith(source), name, attributes);
  }

  @Override
  public Source<T> mergeWith(Publisher<Message<T>>... sources) {
    List<Publisher<Message<T>>> list = new ArrayList<>();
    list.add(this);
    list.addAll(Arrays.asList(sources));
    return new DefaultSource<>(Flowable.merge(list), name, attributes);
  }

  @Override
  public <X> Source<X> compose(Function<Publisher<Message<T>>, Publisher<Message<X>>> mapper) {
    return new DefaultSource<>(asFlowable().compose(mapper::apply), name, attributes);
  }

  @Override
  public <X> Source<X> composeFlowable(Function<Flowable<Message<T>>, Flowable<Message<X>>> mapper) {
    return new DefaultSource<>(asFlowable().compose(mapper::apply), name, attributes);
  }

  @Override
  public <X> Source<X> composePayloadFlowable(Function<Flowable<T>, Flowable<X>> function) {
    return new DefaultSource<>(
      asFlowable().compose(upstream -> Flowable.defer(() -> {
        AtomicReference<Message<T>> current = new AtomicReference<>();
        return function.apply(upstream.doOnNext(current::set).map(Message::payload))
          .map(s -> current.get().with(s));
      })), name, attributes);
  }
}
