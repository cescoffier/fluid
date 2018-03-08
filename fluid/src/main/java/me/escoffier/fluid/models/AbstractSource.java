package me.escoffier.fluid.models;

import io.reactivex.Flowable;
import io.reactivex.Single;
import org.apache.logging.log4j.LogManager;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static me.escoffier.fluid.models.Pair.pair;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class AbstractSource<T> implements Source<T> {

  private final Publisher<Data<T>> flow;

  private final String name;

  private final Map<String, Object> attributes;

  public AbstractSource(Publisher<Data<T>> items, String name, Map<String, Object> attr) {
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
    return new AbstractSource<>(flow, name, attributes);
  }

  @Override
  public Source<T> withAttribute(String key, Object value) {
    Map<String, Object> attr = new HashMap<>(attributes);
    attr.put(key, value);
    return new AbstractSource<>(flow, name, attr);
  }

  @Override
  public void subscribe(Subscriber<? super Data<T>> s) {
    flow.subscribe(s);
  }

  @Override
  public Source<T> orElse(Source<T> alt) {
    return new AbstractSource<>(Flowable.fromPublisher(flow).switchIfEmpty(alt), name, attributes);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Optional<T> attr(String key) {
    return Optional.ofNullable((T) attributes.get(key));
  }

  // TODO dispatchOn to select on which thread pool the source emit the data

  @Override
  public <X> Source<X> map(Function<Data<T>, Data<X>> mapper) {
    Objects.requireNonNull(mapper, "The `mapper` function cannot be `null`");
    return new AbstractSource<>(Flowable.fromPublisher(flow).map(mapper::apply), name, attributes);
  }

  @Override
  public <X> Source<X> mapItem(Function<T, X> mapper) {
    Objects.requireNonNull(mapper, "The `mapper` function cannot be `null`");
    Flowable<Data<X>> flowable = Flowable.fromPublisher(flow).map(d -> d.with(mapper.apply(d.payload())));
    return new AbstractSource<>(flowable, name, attributes);
  }

  @Override
  public Source<T> filter(Predicate<Data<T>> filter) {
    Objects.requireNonNull(filter, "The `filter` function cannot be `null`");
    return new AbstractSource<>(Flowable.fromPublisher(flow).filter(filter::test), name, attributes);
  }

  @Override
  public Source<T> filterPayload(Predicate<T> filter) {
    Objects.requireNonNull(filter, "The `filter` function cannot be `null`");
    Flowable<Data<T>> flowable = Flowable.fromPublisher(flow).filter(d -> filter.test(d.payload()));
    return new AbstractSource<>(flowable, name, attributes);
  }

  @Override
  public Source<T> filterNot(Predicate<Data<T>> filter) {
    Objects.requireNonNull(filter, "The `filter` function cannot be `null`");
    return new AbstractSource<>(Flowable.fromPublisher(flow).filter(d -> !filter.test(d)), name, attributes);
  }

  @Override
  public Source<T> filterPayloadNot(Predicate<T> filter) {
    Objects.requireNonNull(filter, "The `filter` function cannot be `null`");
    Flowable<Data<T>> flowable = Flowable.fromPublisher(flow).filter(d -> !filter.test(d.payload()));
    return new AbstractSource<>(flowable, name, attributes);
  }

  @Override
  public <X> Source<X> flatMap(Function<Data<T>, Publisher<Data<X>>> mapper) {
    Objects.requireNonNull(mapper, "The `mapper` function cannot be `null`");
    return new AbstractSource<>(Flowable.fromPublisher(flow).flatMap(mapper::apply), name, attributes);
  }

  @Override
  public <X> Source<X> concatMap(Function<Data<T>, Publisher<Data<X>>> mapper) {
    Objects.requireNonNull(mapper, "The `mapper` function cannot be `null`");
    return new AbstractSource<>(Flowable.fromPublisher(flow).concatMap(mapper::apply), name, attributes);
  }

  @Override
  public <X> Source<X> flatMap(Function<Data<T>, Publisher<Data<X>>> mapper, int maxConcurrency) {
    Objects.requireNonNull(mapper, "The `mapper` function cannot be `null`");
    if (maxConcurrency < 1) {
      throw new IllegalArgumentException("The `maxConcurrency` cannot be less than 1");
    }
    return new AbstractSource<>(Flowable.fromPublisher(flow).flatMap(mapper::apply, maxConcurrency), name, attributes);
  }

  @Override
  public <X> Source<X> flatMapItem(Function<T, Publisher<X>> mapper) {
    Objects.requireNonNull(mapper, "The `mapper` function cannot be `null`");

    Flowable<Data<X>> flowable = Flowable.fromPublisher(flow)
      .flatMap(data -> {
        Publisher<X> publisher = mapper.apply(data.payload());
        return Flowable.fromPublisher(publisher).map(data::with);
      });

    return new AbstractSource<>(flowable, name, attributes);
  }

  @Override
  public <X> Source<X> concatMapItem(Function<T, Publisher<X>> mapper) {
    Objects.requireNonNull(mapper, "The `mapper` function cannot be `null`");

    Flowable<Data<X>> flowable = Flowable.fromPublisher(flow)
      .concatMap(data -> {
        Publisher<X> publisher = mapper.apply(data.payload());
        return Flowable.fromPublisher(publisher).map(data::with);
      });

    return new AbstractSource<>(flowable, name, attributes);
  }

  @Override
  public <X> Source<X> flatMapItem(Function<T, Publisher<X>> mapper, int maxConcurrency) {
    Objects.requireNonNull(mapper, "The `mapper` function cannot be `null`");

    Flowable<Data<X>> flowable = Flowable.fromPublisher(flow)
      .flatMap(data -> {
        Publisher<X> publisher = mapper.apply(data.payload());
        return Flowable.fromPublisher(publisher).map(data::with);
      }, maxConcurrency);

    return new AbstractSource<>(flowable, name, attributes);
  }

  @Override
  public <X> Source<X> reduce(Data<X> zero, BiFunction<Data<X>, Data<T>, Data<X>> function) {
    Objects.requireNonNull(function, "The `function` cannot be `null`");
    Objects.requireNonNull(zero, "The `zero` item (seed) cannot be `null`");
    Flowable<Data<X>> reduced = Flowable.fromPublisher(flow).reduce(zero, function::apply).toFlowable();
    return new AbstractSource<>(reduced, name, attributes);
  }

  @Override
  public <X> Source<X> reduceItems(X zero, BiFunction<X, T, X> function) {
    Objects.requireNonNull(function, "The `function` cannot be `null`");
    Flowable<Data<X>> reduced = Flowable.fromPublisher(flow)
      .map(Data::payload)
      .reduce(zero, function::apply)
      .map(Data::new)
      .toFlowable();
    return new AbstractSource<>(reduced, name, attributes);
  }

  @Override
  public <X> Source<X> scan(Data<X> zero, BiFunction<Data<X>, Data<T>, Data<X>> function) {
    Objects.requireNonNull(function, "The `function` cannot be `null`");
    Objects.requireNonNull(zero, "The `zero` item (seed) cannot be `null`");
    Flowable<Data<X>> reduced = Flowable.fromPublisher(flow).scan(zero, function::apply);
    return new AbstractSource<>(reduced, name, attributes);
  }

  @Override
  public <X> Source<X> scanItems(X zero, BiFunction<X, T, X> function) {
    Objects.requireNonNull(function, "The `function` cannot be `null`");
    Flowable<Data<X>> reduced = Flowable.fromPublisher(flow)
      .map(Data::payload)
      .scan(zero, function::apply)
      .map(Data::new); // TODO We are loosing the headers
    return new AbstractSource<>(reduced, name, attributes);
  }

  @Override
  public <K> Publisher<GroupedDataStream<K, T>> groupBy(Function<Data<T>, K> keySupplier) {
    return Flowable.fromPublisher(flow)
      .groupBy(keySupplier::apply)
      .flatMapSingle(gf -> {
        GroupedDataStream<K, T> stream = new GroupedDataStream<>(gf.getKey(), gf);
        return Single.just(stream);
      });
  }

  // TODO Windowing here

  public Source<T> log(String loggerName) {
    Flowable<Data<T>> flowable = Flowable.fromPublisher(flow)
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
    return new AbstractSource<>(flowable, name, attributes);
  }

  @Override
  public List<Source<T>> broadcast(int numberOfBranches) {
    if (numberOfBranches <= 1) {
      throw new IllegalArgumentException("The number of branch must be at least 2");
    }

    List<Source<T>> streams = new ArrayList<>(numberOfBranches);
    Flowable<Data<T>> publish = Flowable.fromPublisher(flow).publish().autoConnect(numberOfBranches);

    for (int i = 0; i < numberOfBranches; i++) {
      Source<T> stream = new AbstractSource<>(publish, name, attributes);
      streams.add(stream);
    }

    return streams;
  }

  @Override
  public List<Source<T>> broadcast(String... names) {
    if (names == null || names.length <= 1) {
      throw new IllegalArgumentException("The number of branch must be at least 2");
    }

    List<Source<T>> streams = new ArrayList<>(names.length);
    Flowable<Data<T>> publish = Flowable.fromPublisher(flow).publish().autoConnect(names.length);

    for (String n : names) {
      Source<T> stream = new AbstractSource<>(publish, n, attributes);
      streams.add(stream);
    }

    return streams;
  }

  @Override
  public Pair<Source<T>, Source<T>> branch(Predicate<Data<T>> condition) {
    List<Source<T>> sources = broadcast(2);
    Source<T> left = sources.get(0).filter(condition);
    Source<T> right = sources.get(0).filterNot(condition);
    return pair(left, right);
  }

  @Override
  public Pair<Source<T>, Source<T>> branchOnPayload(Predicate<T> condition) {
    List<Source<T>> sources = broadcast(2);
    Source<T> left = sources.get(0).filterPayload(condition);
    Source<T> right = sources.get(0).filterPayloadNot(condition);
    return pair(left, right);
  }

  // TODO Multi-branch

  @Override
  public Sink<T> to(Sink<T> sink) {
    Flowable<Data<Void>> flowable = Flowable.fromPublisher(flow)
      .flatMapCompletable(sink::dispatch)
      .doOnError(Throwable::printStackTrace)
      .toFlowable();

    flowable.subscribe();
    return sink;
  }

  @Override
  public Flowable<Data<T>> asFlowable() {
    return Flowable.fromPublisher(this);
  }

  @Override
  public <O> Source<Pair<T, O>> zipWith(Publisher<Data<O>> source) {
    return new AbstractSource<>(asFlowable().zipWith(source, (a, b) -> a.with(Pair.pair(a.payload(), b.payload()))),
      name, attributes);
  }

  @Override
  public Source<Tuple> zipWith(Source... sources) {
    Publisher[] publishers = Arrays.stream(sources).map((Function<Source, Flowable>) Source::asFlowable).toArray(Publisher[]::new);
    return zipWith(publishers);
  }

  //  @Override
  public Source<Tuple> zipWith(Publisher<Data>... sources) {

    List<Flowable<Data>> toBeZipped = new ArrayList<>();
    toBeZipped.add(Flowable.fromPublisher(this));
    for (Publisher<Data> p : sources) {
      toBeZipped.add(Flowable.fromPublisher(p));
    }

    Flowable<Data<Tuple>> stream = Flowable.zip(toBeZipped, objects -> {
      List<Object> payloads = new ArrayList<>();
      Data<?> first = null;
      for (Object o : objects) {
        if (!(o instanceof Data)) {
          throw new IllegalArgumentException("Invalid incoming item - " + Data.class.getName() + " expected, received " +
            o.getClass().getName());
        } else {
          if (first == null) {
            first = ((Data) o);
          }
          payloads.add(((Data) o).payload());
        }
      }
      if (first == null) {
        throw new IllegalStateException("Invalid set of stream");
      }
      return first.with(Tuple.tuple(payloads.toArray(new Object[payloads.size()])));
    });

    return new AbstractSource<>(stream, name, attributes);
  }

  @Override
  public Source<T> mergeWith(Publisher<Data<T>> source) {
    return new AbstractSource<>(asFlowable().mergeWith(source), name, attributes);
  }

  @Override
  public Source<T> mergeWith(Publisher<Data<T>>... sources) {
    List<Publisher<Data<T>>> list = new ArrayList<>();
    list.add(this);
    list.addAll(Arrays.asList(sources));
    return new AbstractSource<>(Flowable.merge(list), name, attributes);
  }

  @Override
  public <X> Source<X> compose(Function<Publisher<Data<T>>, Publisher<Data<X>>> mapper) {
    return new AbstractSource<>(asFlowable().compose(mapper::apply), name, attributes);
  }

  @Override
  public <X> Source<X> composeFlowable(Function<Flowable<Data<T>>, Flowable<Data<X>>> mapper) {
    return new AbstractSource<>(asFlowable().compose(mapper::apply), name, attributes);
  }

  @Override
  public <X> Source<X> composeItemFlowable(Function<Flowable<T>, Flowable<X>> function) {
    return new AbstractSource<>(
      asFlowable().compose(upstream -> Flowable.defer(() -> {
        AtomicReference<Data<T>> current = new AtomicReference<>();
        return function.apply(upstream.doOnNext(current::set).map(Data::payload))
          .map(s -> current.get().with(s));
      })), name, attributes);
  }
}
