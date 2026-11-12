package me.xkyrell.batcher;

import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static me.xkyrell.batcher.BatcherTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

abstract class AbstractBatcherTest {

    @Test
    void testBatchRetries() {
        AtomicInteger calls = new AtomicInteger();
        Batcher<Integer, Integer> batcher = this.<Integer, Integer>createBuilder()
                .maxBatchSize(3)
                .maxRetries(1)
                .build(keys -> {
                    if (calls.incrementAndGet() == 1) {
                        return CompletableFuture.failedFuture(new IllegalStateException());
                    }

                    Map<Integer, Integer> values = new HashMap<>();
                    keys.forEach(key -> values.put(key, key * 10));
                    return CompletableFuture.completedFuture(values);
                });

        CompletableFuture<Integer> future = batcher.add(1);
        CompletableFuture<Integer> future2 = batcher.add(2);
        CompletableFuture<Integer> future3 = batcher.add(3);
        assertEquals(10, future.join());
        assertEquals(20, future2.join());
        assertEquals(30, future3.join());
        assertEquals(2, calls.get());
        assertEquals(0, batcher.getPending());
    }

    @Test
    void testUnlimitedBatcher() {
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .build(new BatcherRecordingLoader());

        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            futures.add(batcher.add("key" + i));
        }

        assertEquals(10_000, batcher.getPending());
        assertTrue(futures.stream().noneMatch(CompletableFuture::isDone));
    }

    @Test
    void testFlushWhenBatchIsFull() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxBatchSize(3)
                .build(loader);

        CompletableFuture<Integer> futureA = batcher.add("a");
        CompletableFuture<Integer> futureB = batcher.add("b");
        assertEquals(0, loader.getCalls().size());
        assertEquals(2, getQueued(batcher));

        CompletableFuture<Integer> futureC = batcher.add("c");
        assertEquals(List.of(List.of("a", "b", "c")), loader.getCalls());
        assertEquals(1, futureA.join());
        assertEquals(2, futureB.join());
        assertEquals(3, futureC.join());
        assertEquals(0, batcher.getPending());
        assertEquals(0, getQueued(batcher));
        assertEquals(0, getRunning(batcher));
    }

    @Test
    void testFlushBatches() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .maxBatchSize(2)
                .build(loader);

        for (String key : new String[]{"a", "b", "c", "d", "e"}) {
            batcher.add(key);
        }

        FlushResult<String, Integer> result = batcher.flush().join();
        assertEquals(List.of(
                List.of("a", "b"),
                List.of("c", "d"),
                List.of("e")
        ), loader.getCalls());
        assertEquals(3, result.getUsedKeys());
        assertEquals(
                List.of("a", "b", "c", "d", "e"),
                List.copyOf(result.getSuccessRequests().keySet())
        );
        assertTrue(result.isSuccessful());
        assertEquals(0, batcher.getPending());
    }

    @Test
    void testFlushPartialFailure() {
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .maxBatchSize(2)
                .build(keys -> keys.contains("c")
                        ? CompletableFuture.failedFuture(new IllegalStateException())
                        : CompletableFuture.completedFuture(Map.of("a", 1, "b", 2))
                );

        for (String key : List.of("a", "b", "c", "d")) {
            batcher.add(key);
        }

        FlushResult<String, Integer> result = batcher.flush().join();
        assertEquals(Map.of("a", 1, "b", 2), result.getSuccessRequests());
        assertEquals(Set.of("c", "d"), result.getFailedRequests().keySet());
        assertInstanceOf(IllegalStateException.class, result.getFailedRequests().get("c"));
        assertEquals(2, result.getUsedKeys());
        assertFalse(result.isSuccessful());
        assertFalse(result.isEmpty());
    }

    @Test
    void testPendingLimitFlush() {
        BatcherManualLoader loader = new BatcherManualLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.LAZY)
                .maxBatchSize(3)
                .maxPendingRequests(4)
                .build(loader);

        batcher.add("a");
        batcher.add("b");
        batcher.add("c");
        assertEquals(1, loader.getCalls().size());

        CompletableFuture<Integer> futureD = batcher.add("d");
        assertEquals(2, loader.getCalls().size());
        assertEquals(List.of("d"), loader.getCalls().get(1));
        assertInstanceOf(RejectedExecutionException.class, failure(batcher.add("e")));
        assertEquals(4, batcher.getPending());

        loader.complete(0, Map.of("a", 1, "b", 2, "c", 3));
        assertEquals(1, batcher.getPending());
        assertFalse(futureD.isDone());

        loader.complete(1, Map.of("d", 4));
        assertEquals(4, futureD.join());
        assertEquals(0, batcher.getPending());

        CompletableFuture<Integer> futureE = batcher.add("e");
        assertFalse(futureE.isDone());
        assertEquals(1, batcher.getPending());
    }

    @Test
    void testFlushImmediately() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxBatchSize(3)
                .build(loader);

        CompletableFuture<Integer> futureA = batcher.add("a");
        assertTrue(batcher.flush().join().isEmpty());
        assertEquals(1, getQueued(batcher));
        assertEquals(0, loader.getCalls().size());

        FlushResult<String, Integer> result = batcher.flushImmediately().join();
        assertEquals(Map.of("a", 1), result.getSuccessRequests());
        assertEquals(1, futureA.join());
        assertEquals(List.of(List.of("a")), loader.getCalls());
    }

    @Test
    void testFlushWhenRunning() {
        BatcherManualLoader loader = new BatcherManualLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .build(loader);

        batcher.add("a");
        CompletableFuture<FlushResult<String, Integer>> future = batcher.flush();
        CompletableFuture<FlushResult<String, Integer>> future2 = batcher.flush();
        assertEquals(1, loader.getCalls().size());
        assertTrue(future2.isDone());
        assertTrue(future2.join().isEmpty());
        assertFalse(future.isDone());

        loader.complete(0, Map.of("a", 1));
        assertEquals(Map.of("a", 1), future.join().getSuccessRequests());
    }

    @Test
    void testAddDuringCompletion() {
        BatcherRecordingLoader loader = new BatcherRecordingLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .build(loader);

        AtomicReference<CompletableFuture<Integer>> inner = new AtomicReference<>();
        batcher.add("a").thenAccept(__ -> inner.set(batcher.add("b")));
        batcher.flush();
        assertEquals(List.of(List.of("a")), loader.getCalls());
        assertFalse(inner.get().isDone());

        batcher.flush();
        assertEquals(List.of(List.of("a"), List.of("b")), loader.getCalls());
        assertEquals(2, inner.get().join());
    }

    @Test
    void testClose() {
        BatcherManualLoader loader = new BatcherManualLoader();
        BatchRecordingListener listener = new BatchRecordingListener();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .completionListener(listener)
                .build(loader);

        CompletableFuture<Integer> runningFuture = batcher.add("a");
        batcher.flush();
        CompletableFuture<Integer> queuedFuture = batcher.add("b");

        batcher.close();
        batcher.close();
        assertTrue(queuedFuture.isCancelled());
        assertFalse(runningFuture.isDone());
        assertEquals(List.of("b:TERMINATED"), listener.getRecords());

        loader.complete(0, Map.of("a", 1));
        assertEquals(1, runningFuture.join());
        assertEquals(List.of("b:TERMINATED", "a:SUCCESS"), listener.getRecords());
        assertEquals(0, batcher.getPending());
        assertInstanceOf(IllegalStateException.class, failure(batcher.add("a")));
    }

    @Test
    void testClosePreventsRetry() {
        BatcherManualLoader loader = new BatcherManualLoader();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .flushPolicy(BatchFlushPolicy.MANUAL)
                .maxRetries(3)
                .build(loader);

        CompletableFuture<Integer> future = batcher.add("a");
        batcher.flush();
        batcher.close();

        IllegalStateException ex = new IllegalStateException();
        loader.completeExceptionally(0, ex);
        assertSame(ex, failure(future));
        assertEquals(1, loader.getCalls().size());
        assertEquals(0, batcher.getPending());
    }

    @Test
    void testLoaderFailure() {
        BatchRecordingListener listener = new BatchRecordingListener();
        IllegalStateException ex = new IllegalStateException();
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxBatchSize(2)
                .completionListener(listener)
                .build(__ -> CompletableFuture.failedFuture(ex));

        CompletableFuture<Integer> futureA = batcher.add("a");
        CompletableFuture<Integer> futureB = batcher.add("b");

        assertSame(ex, failure(futureA));
        assertSame(ex, failure(futureB));
        assertEquals(List.of("a:FAILED", "b:FAILED"), listener.getRecords());
        assertEquals(0, batcher.getPending());
    }

    @Test
    void testListenerFailure() {
        Batcher<String, Integer> batcher = this.<String, Integer>createBuilder()
                .maxBatchSize(2)
                .completionListener((key, __, ___) -> {
                    throw new IllegalStateException("listener for " + key);
                })
                .build(new BatcherRecordingLoader());

        CompletableFuture<Integer> futureA = batcher.add("a");
        CompletableFuture<Integer> futureB = batcher.add("b");
        assertEquals(1, futureA.join());
        assertEquals(2, futureB.join());
    }

    protected abstract <K, V> Batcher.Builder<K, V, ?> createBuilder();

}
