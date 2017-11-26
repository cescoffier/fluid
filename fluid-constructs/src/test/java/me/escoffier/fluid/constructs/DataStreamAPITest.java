package me.escoffier.fluid.constructs;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class DataStreamAPITest {

    @Test
    public void testTransformWith() {
        CacheSink<Integer> sink = new CacheSink<>();

        Source.from(1, 2, 3, 4, 5)
            .transformWith(x -> x.map(d -> d + 1))
            .to(sink);

        await().until(() -> sink.buffer.size() == 5);
        assertThat(sink.cache()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testTransform() {
        CacheSink<Integer> sink = new CacheSink<>();

        Source.from(1, 2, 3, 4, 5)
            .transform(d -> d + 1)
            .to(sink);

        await().until(() -> sink.buffer.size() == 5);
        assertThat(sink.cache()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    public void testTransformFlow() {
        CacheSink<Integer> sink = new CacheSink<>();

        Source.from(1, 2, 3, 4, 5)
            .transformFlow(f -> f.map(x -> x * 2))
            .to(sink);

        await().until(() -> sink.buffer.size() == 5);
        assertThat(sink.cache()).containsExactly(2, 4, 6, 8, 10);
    }

    @Test
    public void testChainingTransformation() {
        CacheSink<Integer> sink = new CacheSink<>();

        java.util.stream.Stream<Integer> stream = java.util.stream.Stream.iterate(0, i -> i + 1)
            .skip(10)
            .limit(10);
        Source.from(stream)
            .transform(x -> x + 1)
            .transformFlow(in -> in.map(i -> i * 2))
            .to(sink);

        await().until(() -> sink.buffer.size() >= 5);
        assertThat(sink.cache()).containsExactly(22, 24, 26, 28, 30, 32, 34, 36, 38, 40);
    }

}
