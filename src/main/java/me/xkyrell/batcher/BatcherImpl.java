package me.xkyrell.batcher;

import org.jetbrains.annotations.NotNull;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static me.xkyrell.batcher.BatchCompletionListener.Cause.TERMINATED;
import static me.xkyrell.batcher.BatcherExceptions.*;

final class BatcherImpl<K, V> extends AbstractBatcher<K, V> {

    BatcherImpl(
            BatchCompletionListener<K, V> onListener,
            BatchFlushPolicy flushPolicy,
            int maxBatchSize,
            int maxPendingRequests,
            int maxRetries,
            BatchLoader<K, V> onLoader
    ) {
        super(onListener, flushPolicy, maxBatchSize, maxPendingRequests, maxRetries, onLoader);
    }

    @Override
    public int getPending() {
        return pending.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        for (Entry<K, V> entry : prepareEntries()) {
            if (untrack(entry)) {
                entry.completeExceptionally(closed());
                handleListener(entry.getKey(), null, TERMINATED);
            }
        }
    }

    private boolean untrack(Entry<K, V> entry) {
        Entry.State left = entry.terminate(false);
        if (left == null) return false;

        pending.decrementAndGet();
        if (left == Entry.State.QUEUED) {
            queued.decrementAndGet();
        }

        return true;
    }

    @Override
    public CompletableFuture<V> enqueue(@NotNull K key) {
        if (closed.get()) {
            return Entry.<K, V>rejected(key, closed()).getCreatorFuture();
        }

        if (!tryReservePending()) {
            return Entry.<K, V>rejected(key, limitExceeded()).getCreatorFuture();
        }

        Entry<K, V> entry = Entry.shared(key, Entry.State.QUEUED);
        try {
            getEntryQueueInternal().push(entry);
        }
        catch (RuntimeException | Error ex) {
            pending.decrementAndGet();
            throw queueFailed(ex);
        }

        queued.incrementAndGet();
        if (closed.get() && untrack(entry)) {
            entry.completeExceptionally(closed());
            handleListener(entry.getKey(), null, TERMINATED);
        }

        return entry.getCreatorFuture();
    }

    @Override
    public boolean tryComplete(@NotNull Entry<K, V> entry) {
        if (!entry.changeState(Entry.State.RUNNING, Entry.State.COMPLETED)) {
            return false;
        }

        pending.decrementAndGet();
        return true;
    }

    static final class BuilderImpl<K, V> extends AbstractBatcherBuilder<K, V, Batcher<K, V>> {

        @Override
        public Batcher<K, V> build(@NotNull BatchLoader<K, V> onLoader) {
            Objects.requireNonNull(onLoader, "onLoader cannot be null");
            return new BatcherImpl<>(
                    getCompletionListener(), getFlushPolicy(),
                    getMaxBatchSize(), getMaxPendingRequests(),
                    getMaxRetries(), onLoader
            );
        }
    }
}
