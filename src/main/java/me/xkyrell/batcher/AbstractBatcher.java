package me.xkyrell.batcher;

import org.jetbrains.annotations.NotNull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract class AbstractBatcher<K, V> implements Batcher<K, V> {

    private static final Logger log = Logger.getLogger(AbstractBatcher.class.getName());

    protected final AtomicInteger pending = new AtomicInteger();
    protected final AtomicInteger queued = new AtomicInteger();
    protected final AtomicBoolean closed = new AtomicBoolean();
    private final EntryQueue<K, V> queue;
    private final BatchRunner<K, V> runner;
    private final BatchCompletionListener<K, V> onListener;
    private final BatchFlushPolicy flushPolicy;
    private final int maxBatchSize;
    private final int maxPendingRequests;
    private final int maxRetries;

    protected AbstractBatcher(
            BatchCompletionListener<K, V> onListener,
            BatchFlushPolicy flushPolicy,
            int maxBatchSize,
            int maxPendingRequests,
            int maxRetries,
            BatchLoader<K, V> onLoader
    ) {
        this.queue = new EntryQueue<>(queued::get);
        this.runner = new BatchRunner<>(onLoader, this::handleRequest, closed::get);
        this.onListener = onListener;
        this.flushPolicy = flushPolicy;
        this.maxBatchSize = maxBatchSize;
        this.maxPendingRequests = maxPendingRequests;
        this.maxRetries = maxRetries;
    }

    private void handleRequest(Entry<K, V> entry, V value, Throwable cause) {
        if (!tryComplete(entry)) return;
        if (cause == null) {
            entry.complete(value);
            handleListener(entry.getKey(), value, BatchCompletionListener.Cause.SUCCESS);
            return;
        }

        entry.completeExceptionally(cause);
        handleListener(entry.getKey(), null, cause instanceof KeyNotLoadedException
                ? BatchCompletionListener.Cause.DROPPED
                : BatchCompletionListener.Cause.FAILED
        );
    }

    protected final void handleListener(K key, V value, BatchCompletionListener.Cause cause) {
        if (onListener == null) return;

        try {
            onListener.onCompletion(key, value, cause);
        }
        catch (Exception ex) {
            log.log(Level.WARNING, "Exception thrown by removal listener", ex);
        }
    }

    @Override
    public final CompletableFuture<V> add(@NotNull K key) {
        Objects.requireNonNull(key, "key cannot be null");

        CompletableFuture<V> caller = enqueue(key);
        if (flushPolicy == BatchFlushPolicy.LAZY && isReady()) {
            runner.runBatches(prepareEntries(), maxBatchSize, maxRetries);
        }

        return caller;
    }

    @Override
    public final CompletableFuture<FlushResult<K, V>> flush() {
        if (flushPolicy == BatchFlushPolicy.LAZY && !isReady() && !closed.get()) {
            return CompletableFuture.completedFuture(FlushResultImpl.empty());
        }

        return flushImmediately();
    }

    @Override
    public final CompletableFuture<FlushResult<K, V>> flushImmediately() {
        if (closed.get()) {
            return CompletableFuture.failedFuture(BatcherExceptions.closed());
        }

        if (queued.get() <= 0) {
            return CompletableFuture.completedFuture(FlushResultImpl.empty());
        }

        return runner.runBatches(prepareEntries(), maxBatchSize, maxRetries);
    }

    protected final List<Entry<K, V>> prepareEntries() {
        List<Entry<K, V>> entries = queue.detach(Math.max(0, queued.get()));
        if (!entries.isEmpty()) {
            queued.addAndGet(-entries.size());
        }

        return entries;
    }

    private boolean isReady() {
        int waiting = queued.get();
        boolean limitReached = maxPendingRequests > 0
                && pending.get() >= maxPendingRequests;

        return (waiting >= maxBatchSize) || (limitReached && waiting > 0);
    }

    protected final boolean tryReservePending() {
        if (maxPendingRequests == 0) {
            pending.incrementAndGet();
            return true;
        }

        while (true) {
            int current = pending.get();
            if (current >= maxPendingRequests) {
                return false;
            }

            if (pending.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    protected final EntryQueue<K, V> getEntryQueueInternal() {
        return queue;
    }

    public abstract CompletableFuture<V> enqueue(@NotNull K key);

    public abstract boolean tryComplete(@NotNull Entry<K, V> entry);

}
