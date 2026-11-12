package me.xkyrell.batcher;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DeduplicatingBatcherTest extends AbstractBatcherTest {

    @Test
    void testDuplicateRequests() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .build(loader);

        CompletableFuture<Integer> future = batcher.add("a");
        CompletableFuture<Integer> future2 = batcher.add("a");

        assertNotSame(future, future2);
        assertEquals(1, batcher.getPending());
        batcher.flush();

        assertEquals(List.of(List.of("a")), loader.getCalls());
        assertEquals(1, future.join());
        assertEquals(1, future2.join());
    }

    @Test
    void testCancellationIsolation() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .build(loader);

        CompletableFuture<Integer> future = batcher.add("a");
        CompletableFuture<Integer> future2 = batcher.add("a");
        future.cancel(true);
        batcher.flush();

        assertEquals(1, future2.join());
        assertEquals(1, loader.getCalls().size());
    }

    @Test
    void testDeduplicateRunningState() {
        BatcherManualLoader loader = new BatcherManualLoader();
        DeduplicatingBatcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxBatchSize(2)
                .build(loader);

        CompletableFuture<Integer> futureA = batcher.add("a");
        CompletableFuture<Integer> futureB = batcher.add("b");
        assertEquals(1, loader.getCalls().size());

        CompletableFuture<Integer> futureA2 = batcher.add("a");
        CompletableFuture<Integer> futureC = batcher.add("c");
        assertEquals(1, loader.getCalls().size());
        assertTrue(batcher.contains("a"));

        loader.complete(0, Map.of("a", 10, "b", 20));
        assertEquals(10, futureA.join());
        assertEquals(10, futureA2.join());
        assertEquals(20, futureB.join());
        assertFalse(futureC.isDone());
    }

    @Test
    void testRededuplication() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        DeduplicatingBatcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxBatchSize(1)
                .build(loader);

        assertEquals(1, batcher.add("a").join());
        assertFalse(batcher.contains("a"));
        assertEquals(1, batcher.add("a").join());
        assertEquals(2, loader.getCalls().size());
    }

    @Test
    void testConcurrentDeduplication() throws Exception {
        ExecutorService callers = Executors.newFixedThreadPool(8);
        try {
            BatcherManualLoader loader = new BatcherManualLoader();
            Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                    .flushPolicy(BatchFlushPolicy.MANUAL)
                    .maxPendingRequests(1)
                    .build(loader);

            CountDownLatch start = new CountDownLatch(1);
            List<CompletableFuture<Integer>> futures = new CopyOnWriteArrayList<>();
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                tasks.add(callers.submit(() -> {
                    start.await();
                    futures.add(batcher.add("same"));
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> task : tasks) {
                task.get(3, TimeUnit.SECONDS);
            }
            assertEquals(8, futures.size());
            assertEquals(1, batcher.getPending());

            batcher.flush();
            assertEquals(List.of("same"), loader.getCalls().get(0));

            loader.complete(0, Map.of("same", 4));
            for (CompletableFuture<Integer> future : futures) {
                assertEquals(4, future.join());
            }
            assertEquals(0, batcher.getPending());
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    void testRemoveQueuedRequest() {
        BatchRecordingListener listener = new BatchRecordingListener();
        DeduplicatingBatcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .completionListener(listener)
                .build(new BatcherRecordingLoader());

        CompletableFuture<Integer> future = batcher.add("a");
        assertTrue(batcher.remove("a"));
        assertFalse(batcher.remove("a"));
        assertTrue(future.isCancelled());
        assertFalse(batcher.contains("a"));
        assertEquals(0, batcher.getPending());
        assertEquals(List.of("a:EXPLICIT"), listener.getRecords());
    }

    @Test
    void testRemoveRunningRequest() {
        BatcherManualLoader loader = new BatcherManualLoader();
        BatchRecordingListener listener = new BatchRecordingListener();
        DeduplicatingBatcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxBatchSize(1)
                .completionListener(listener)
                .build(loader);

        CompletableFuture<Integer> future = batcher.add("a");
        assertTrue(batcher.remove("a"));
        loader.complete(0, Map.of("a", 1));

        assertTrue(future.isCancelled());
        assertEquals(List.of("a:EXPLICIT"), listener.getRecords());
        assertEquals(0, batcher.getPending());
    }

    @Test
    void testRemoveRetryChain() {
        BatcherManualLoader loader = new BatcherManualLoader();
        DeduplicatingBatcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxRetries(3)
                .build(loader);

        CompletableFuture<Integer> future = batcher.add("a");
        batcher.flushImmediately();
        assertTrue(batcher.remove("a"));

        loader.completeExceptionally(0, new IllegalStateException());
        assertTrue(future.isCancelled());
        assertEquals(1, loader.getCalls().size());
        assertEquals(0, batcher.getPending());
    }

    @Test
    void testRemoveAll() {
        BatcherManualLoader loader = new BatcherManualLoader();
        DeduplicatingBatcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxBatchSize(2)
                .build(loader);

        CompletableFuture<Integer> futureA = batcher.add("a");
        batcher.add("b");
        CompletableFuture<Integer> futureC = batcher.add("c");
        assertEquals(3, batcher.removeAll());
        assertTrue(futureA.isCancelled());
        assertTrue(futureC.isCancelled());
        assertEquals(0, batcher.removeAll());
        assertEquals(0, batcher.getPending());
    }

    @Test
    void testBatchSize() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxBatchSize(2)
                .build(loader);

        batcher.add("a");
        batcher.add("a");
        batcher.add("a");
        assertEquals(0, loader.getCalls().size());
        assertEquals(1, batcher.getPending());
    }

    @Test
    void testDeduplicationAcrossRetries() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        var firstAttempt = new AtomicReference<CompletableFuture<Map<String, Integer>>>();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxRetries(1)
                .build(keys -> {
                    if (attempts.incrementAndGet() == 1) {
                        CompletableFuture<Map<String, Integer>> future = new CompletableFuture<>();
                        firstAttempt.set(future);
                        return future;
                    }
                    return CompletableFuture.completedFuture(Map.of("a", 5));
                });

        CompletableFuture<Integer> futureA = batcher.add("a");
        CompletableFuture<FlushResult<String, Integer>> resultFuture = batcher.flushImmediately();
        CompletableFuture<Integer> futureA2 = batcher.add("a");
        firstAttempt.get().completeExceptionally(new IllegalStateException());

        FlushResult<String, Integer> result = resultFuture.join();
        assertEquals(5, futureA.join());
        assertEquals(5, futureA2.join());
        assertEquals(2, attempts.get());
        assertEquals(2, result.getUsedKeys());
        assertEquals(0, batcher.getPending());
    }

    @Override
    protected final <K, V> DeduplicatingBatcher.Builder<K, V, DeduplicatingBatcher<K, V>> createBuilder() {
        return DeduplicatingBatcher.builder();
    }
}
