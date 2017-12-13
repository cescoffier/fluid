package me.escoffier.fluid;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.*;
import org.junit.Test;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SimpleTest {


  @Test
  public void test() {
    Flowable<Integer> range = Flowable.range(1, 10);
    ListSink<Integer> sink = Sink.list();

    Source.fromPayloads(range).windowBySize(2)
//      .transformFlow(flow -> {
//        return flow.lift(
//          (FlowableOperator<Data<Integer>, Data<Integer>>) observer -> new Subscriber<Data<Integer>>() {
//            Subscription subscription;
//            @Override
//            public void onSubscribe(Subscription s) {
//              System.out.println("Subscription...");
//              if (subscription != null) {
//                observer.onSubscribe(subscription);
//                return;
//              }
//              subscription = new Subscription() {
//                @Override
//                public void request(long n) {
//                  s.request(n);
//                }
//
//                @Override
//                public void cancel() {
//                  System.out.println("Cancellation received, ignoring");
//                }
//              };
//              observer.onSubscribe(subscription);
//            }
//
//            @Override
//            public void onNext(Data<Integer> integerData) {
//              observer.onNext(integerData);
//            }
//
//            @Override
//            public void onError(Throwable t) {
//              observer.onError(t);
//            }
//
//            @Override
//            public void onComplete() {
//              observer.onComplete();
//            }
//          });
//      })
      .onPayload(d ->
        System.out.println("Getting " + d + " with window: " + FlowContext.window()))
      .transformPayload(i -> {
        if (i == 5) {
          throw new IllegalArgumentException("BOOM");
        }
        return i;
      })
//      .transformFlow(flow -> flow.retry(t -> {
//        System.out.println(t.getMessage());
//        FlowContext.set("error", t);
//        return true;
//      }))
      .transformFlow(flow -> {
        return flow.onErrorResumeNext(t -> {
          FlowContext.set("error", t);
          return Flowable.empty();
        });
      })
      .to(sink);

    System.out.println(sink.values());
  }

  @Test
  public void test3() {
    ListSink<Integer> sink = Sink.list();
    Source.from(1, 2, 3, 4, 5, 6, 7)
      .windowBySize(3)
      .onData(i -> System.out.println(i))
      .transformPayload(i -> i + 1)
      .transformPayload(i -> {
        if (i == 4) {
          throw new RuntimeException("BOOM");
        }
        return i;
      })
      .to(sink);

    System.out.println("Sink: " + sink.values());
  }

  @Test
  public void test4() {
    ListSink<String> sink1 = Sink.list();
    ListSink<String> sink2 = Sink.list();
    Pair<DataStream<Integer>, DataStream<Integer>> branches = Source.from(2, 2, 3, 4, 5, 6, 7)
      .windowBySize(2)
//      .transformPayload(i -> i + 1)
      .branchOnPayload(i -> i % 2 == 0);

    branches.left().transformPayload(i -> Integer.toString(i)).to(sink1);
    branches.right().transformPayload(i -> Integer.toString(i))
      .transformPayloadFlow(f -> f.doOnError(t -> t.printStackTrace()))
      .onData(s -> System.out.println("Contain " + s))
      .to(sink2);

    System.out.println("Sink1: " + sink1.values());
    System.out.println("Sink2: " + sink2.values());
  }

  @Test
  public void test5() {
    ListSink<String> sink1 = Sink.list();
    Pair<DataStream<Integer>, DataStream<Integer>> branches = Source.from(2, 2, 3, 4, 5, 6, 7)
      .windowBySize(2)
//      .transformPayload(i -> i + 1)
      .branchOnPayload(i -> i % 2 == 0);

    DataStream<String> left = branches.left().transformPayload(i -> Integer.toString(i));
    branches.right().transformPayload(i -> Integer.toString(i))
      .mergeWith(left)
      .to(sink1);

    System.out.println("Sink1: " + sink1.values());
  }

  @Test
  public void test6() {
    Source<String> letters = Source.from("a", "b", "c").windowBySize(2);
    Source<Integer> numbers = Source.from(1, 2, 3);

    letters.transformPayload(s -> s.toUpperCase())
      .onData(d -> System.out.println(d))
      .zipWith(numbers.transformPayload(i -> i + 1))
      .onData(d -> System.out.println(d))
      .transformPayload(p -> p.left() + ":" + p.right())
      .to(Sink.forEach(System.out::println));
  }

  @Test
  public void test7() {
    Source<String> letters = Source.from("a", "b", "c").windowBySize(3);
    Source<String> letters2 = Source.from("d", "e", "F");

    letters.transformPayload(s -> s.toUpperCase())
      .onData(d -> System.out.println(d))
      .mergeWith(letters2.transformPayload(s -> s.toUpperCase()))
      .onData(d -> System.out.println(d))
      .to(Sink.forEach(System.out::println));
  }
}
