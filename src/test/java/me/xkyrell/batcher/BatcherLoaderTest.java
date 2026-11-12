package me.xkyrell.batcher;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static me.xkyrell.batcher.BatcherTestSupport.failure;
import static org.junit.jupiter.api.Assertions.*;

class BatcherLoaderTest {

    @Test
    void testOrderedRequests() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = builder().build(loader);
        batcher.add("c");
        batcher.add("a");
        batcher.add("b");

        batcher.flush().join();
        assertEquals(List.of(List.of("c", "a", "b")), loader.getCalls());
    }

    @Test
    void testMissingKeys() {
        BatchRecordingListener listener = new BatchRecordingListener();
        Batcher<String, Integer> batcher = builder()
                .completionListener(listener)
                .build(BatchLoader.wrap(keys -> Map.of("a", 1)));

        CompletableFuture<Integer> foundFuture = batcher.add("a");
        CompletableFuture<Integer> missingFuture = batcher.add("b");
        FlushResult<String, Integer> resultFuture = batcher.flush().join();
        assertEquals(1, foundFuture.join());

        KeyNotLoadedException ex = assertInstanceOf(KeyNotLoadedException.class, failure(missingFuture));
        assertTrue(ex.getMessage().contains("b"));
        assertEquals(Map.of("a", 1), resultFuture.getSuccessRequests());
        assertEquals(List.of("a:SUCCESS", "b:DROPPED"), listener.getRecords());
        assertEquals(0, batcher.getPending());
    }

    @Test
    void testNoRetryMissingKey() {
        BatcherRecordingLoader calls = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = builder()
                .maxRetries(3)
                .build(keys -> {
                    calls.load(keys);
                    return CompletableFuture.completedFuture(Map.of());
                });

        CompletableFuture<Integer> future = batcher.add("a");
        batcher.flush().join();
        assertInstanceOf(KeyNotLoadedException.class, failure(future));
        assertEquals(1, calls.getCalls().size());
    }

    @Test
    void testNullLoaderResult() {
        Batcher<String, Integer> batcher = builder().build(
                keys -> CompletableFuture.completedFuture(null)
        );

        CompletableFuture<Integer> future = batcher.add("a");
        batcher.flush().join();
        assertInstanceOf(NullPointerException.class, failure(future));
    }

    @Test
    void testNullLoaderStage() {
        Batcher<String, Integer> batcher = builder().build(keys -> null);
        CompletableFuture<Integer> future = batcher.add("a");
        batcher.flush().join();
        assertInstanceOf(NullPointerException.class, failure(future));
    }

    @Test
    void testSyncRequests() {
        IllegalStateException ex = new IllegalStateException();
        Batcher<String, Integer> batcher = builder().build(keys -> {
            throw ex;
        });

        CompletableFuture<Integer> future = batcher.add("a");
        FlushResult<String, Integer> result = batcher.flush().join();
        assertSame(ex, failure(future));
        assertSame(ex, result.getFailedRequests().get("a"));
    }

    @Test
    void testCompletionRequestsUnwrapped() {
        IllegalStateException ex = new IllegalStateException();
        Batcher<String, Integer> batcher = builder().build(
                keys -> CompletableFuture.failedFuture(new CompletionException(ex))
        );

        CompletableFuture<Integer> future = batcher.add("a");
        batcher.flush().join();
        assertSame(ex, failure(future));
    }

    @Test
    void testCancelledStageRequests() {
        BatcherManualLoader loader = new BatcherManualLoader();
        Batcher<String, Integer> batcher = builder()
                .maxRetries(0)
                .build(loader);

        CompletableFuture<Integer> future = batcher.add("a");
        CompletableFuture<FlushResult<String, Integer>> flush = batcher.flush();
        CompletableFuture<Map<String, Integer>> stage = loader.getStages().get(0);
        assertTrue(stage.cancel(false));
        assertInstanceOf(CancellationException.class, failure(future));
        assertEquals(0, batcher.getPending());
        assertTrue(flush.isDone());
        assertEquals(1, flush.join().getUsedKeys());
    }

    @Test
    void testAsyncFailure() {
        BatcherManualLoader loader = new BatcherManualLoader();
        Batcher<String, Integer> batcher = builder()
                .maxRetries(1)
                .build(loader);

        CompletableFuture<Integer> future = batcher.add("a");
        CompletableFuture<FlushResult<String, Integer>> flush = batcher.flush();
        loader.completeExceptionally(0, new IllegalStateException());
        assertEquals(2, loader.getCalls().size());
        assertFalse(future.isDone());

        loader.complete(1, Map.of("a", 7));
        assertEquals(7, future.join());
        assertEquals(2, flush.join().getUsedKeys());
    }

    @Test
    void testLimitedRetries() {
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException ex = new IllegalStateException();
        Batcher<String, Integer> batcher = builder()
                .maxRetries(2)
                .build(keys -> {
                    calls.incrementAndGet();
                    return CompletableFuture.failedFuture(ex);
                });

        CompletableFuture<Integer> future = batcher.add("a");
        FlushResult<String, Integer> result = batcher.flush().join();
        assertSame(ex, failure(future));
        assertEquals(3, calls.get());
        assertEquals(3, result.getUsedKeys());
    }

    @Test
    void testIndependentBatches() {
        Batcher<String, Integer> batcher = builder()
                .maxBatchSize(2)
                .build(keys -> keys.contains("c")
                        ? CompletableFuture.failedFuture(new IllegalStateException())
                        : CompletableFuture.completedFuture(Map.of("a", 1, "b", 2))
                );

        CompletableFuture<Integer> futureA = batcher.add("a");
        CompletableFuture<Integer> futureB = batcher.add("b");
        CompletableFuture<Integer> futureC = batcher.add("c");
        batcher.flush().join();
        assertEquals(1, futureA.join());
        assertEquals(2, futureB.join());
        assertInstanceOf(IllegalStateException.class, failure(futureC));
    }

    private Batcher.Builder<String, Integer, ?> builder() {
        return Batcher.<String, Integer>builder().flushPolicy(BatchFlushPolicy.MANUAL);
    }
}

