package me.escoffier.fluid.constructs.impl;

import io.reactivex.Flowable;
import me.escoffier.fluid.constructs.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the behavior of the branch / conditional constructs.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class BranchTest {


  @Test
  public void testIfThenElse() {
    ListSink<String> l1 = Sink.list();
    ListSink<Double> l2 = Sink.list();

    Pair<DataStream<Integer>, DataStream<Integer>> branch = Source
      .fromItems(Flowable.range(0, 10))
      .branchOnItem(x -> x < 5);

    branch.left().transformItem(i -> Integer.toString(i)).to(l1);
    branch.right().transformItem(i -> i * 2.0).to(l2);

    assertThat(l1.values()).containsExactly("0", "1", "2", "3", "4");
    assertThat(l2.values()).containsExactly(10.0, 12.0, 14.0, 16.0, 18.0);
  }

  @Test
  public void testSplitAndMerge() {
    ListSink<String> l1 = Sink.list();

    Pair<DataStream<Integer>, DataStream<Integer>> branch = Source
      .fromItems(Flowable.range(0, 10))
      .branchOnItem(x -> x < 5);


    DataStream<Double> stream = branch.left().transformItem(i -> i * 1.0);
    branch.right().transformItem(i -> i * 2.0).mergeWith(stream).transformItem(Object::toString).to(l1);

    assertThat(l1.values()).containsExactly("0.0", "1.0", "2.0", "3.0", "4.0", "10.0", "12.0", "14.0", "16.0", "18.0");
  }

  @Test
  public void testWithTwoConditionsOnItems() {
    ListSink<String> l1 = Sink.list();

    List<DataStream<Integer>> branches = Source
      .fromItems(Flowable.range(0, 10))
      .branchOnItem(x -> x < 3, x -> x < 8);

    DataStream<String> stream1 = branches.get(0).transformItem(i -> Integer.toString(i));
    DataStream<String> stream2 = branches.get(1).transformItem(i -> i * 2.0).transformItem(x -> Double.toString(x));

    stream1.mergeWith(stream2).to(l1);

    assertThat(l1.values()).containsExactly("0", "1", "2", "6.0", "8.0", "10.0", "12.0", "14.0");
  }

  @Test
  public void testIfThenElseOnData() {
    ListSink<String> l1 = Sink.list();

    List<Data<String>> data = Arrays.asList(
      new Data<>("a").with("up", true),
      new Data<>("b").with("up", false),
      new Data<>("c").with("up", true),
      new Data<>("d").with("up", true),
      new Data<>("e").with("up", false)
    );

    Pair<DataStream<String>, DataStream<String>> branches = Source.from(data)
      .branch(d -> d.get("up"));

    branches.left().transformItem(String::toUpperCase).mergeWith(branches.right()).to(l1);

    assertThat(l1.values()).containsExactly("A", "b", "C", "D", "e");
  }

  @Test
  public void testMultipleBranchOnData() {
    ListSink<String> l1 = Sink.list();

    List<Data<String>> data = Arrays.asList(
      new Data<>("a").with("up", 1),
      new Data<>("b").with("up", 0),
      new Data<>("c").with("up", 2),
      new Data<>("d").with("up", 2),
      new Data<>("e").with("up", 1),
      new Data<>("f").with("up", 0)
    );

    List<DataStream<String>> branches = Source.from(data)
      .branch(d -> ((int) d.get("up")) == 1,  d -> ((int) d.get("up")) == 2);

    DataStream<String> stream = branches.get(0).transformItem(String::toUpperCase);
    branches.get(1).transformItem(x -> "--" + x + "--").mergeWith(stream).to(l1);

    assertThat(l1.values()).containsExactly("A", "--c--", "--d--", "E");
  }

}
