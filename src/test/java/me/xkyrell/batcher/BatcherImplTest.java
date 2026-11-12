package me.xkyrell.batcher;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatcherImplTest extends AbstractBatcherTest {

    @Test
    void testDuplicateKeys() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .build(loader);

        CompletableFuture<Integer> futureA = batcher.add("a");
        CompletableFuture<Integer> futureA2 = batcher.add("a");
        CompletableFuture<Integer> futureB = batcher.add("b");
        assertEquals(3, batcher.getPending());

        batcher.flush();
        assertEquals(List.of(List.of("a", "a", "b")), loader.getCalls());
        assertEquals(1, futureA.join());
        assertEquals(1, futureA2.join());
        assertEquals(2, futureB.join());
        assertEquals(0, batcher.getPending());
    }

    @Test
    void testBatchSize() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxBatchSize(2)
                .build(loader);

        batcher.add("a");
        assertEquals(0, loader.getCalls().size());

        batcher.add("a");
        batcher.add("a");
        assertEquals(List.of(List.of("a", "a")), loader.getCalls());
    }

    @Override
    protected final <K, V> Batcher.Builder<K, V, ?> createBuilder() {
        return Batcher.builder();
    }
}
