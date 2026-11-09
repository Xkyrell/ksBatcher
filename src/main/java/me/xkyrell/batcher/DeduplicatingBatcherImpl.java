package me.xkyrell.batcher;

import me.xkyrell.batcher.internal.Futures;
import org.jetbrains.annotations.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static me.xkyrell.batcher.BatchCompletionListener.Cause.EXPLICIT;
import static me.xkyrell.batcher.BatchCompletionListener.Cause.TERMINATED;
import static me.xkyrell.batcher.BatcherExceptions.*;

final class DeduplicatingBatcherImpl<K, V>
        extends AbstractBatcher<K, V>
        implements DeduplicatingBatcher<K, V> {

    private final Map<K, Entry<K, V>> indexes = new ConcurrentHashMap<>();

    private DeduplicatingBatcherImpl(
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
    public boolean contains(@NotNull K key) {
        Objects.requireNonNull(key, "key cannot be null");

        Entry<K, V> entry = indexes.get(key);
        return entry != null && !entry.getState().isTerminal();
    }

    @Override
    public boolean remove(@NotNull K key) {
        Objects.requireNonNull(key, "key cannot be null");

        Entry<K, V> entry = indexes.get(key);
        if (entry == null || !removeInternal(entry, false)) {
            return false;
        }

        getEntryQueueInternal().compact();
        entry.completeExceptionally(requestRemoved());
        handleListener(entry.getKey(), null, EXPLICIT);
        return true;
    }

    private boolean removeInternal(Entry<K, V> entry, boolean skipRunning) {
        Entry.State left = entry.terminate(skipRunning);
        if (left == null) return false;

        indexes.remove(entry.getKey(), entry);
        if (left != Entry.State.PUBLISHING) {
            pending.decrementAndGet();
        }

        if (left == Entry.State.QUEUED) {
            queued.decrementAndGet();
        }

        return true;
    }

    @Override
    public int removeAll() {
        List<Entry<K, V>> removed = terminateAll(false);
        getEntryQueueInternal().compact();
        for (Entry<K, V> entry : removed) {
            entry.completeExceptionally(requestRemoved());
            handleListener(entry.getKey(), null, EXPLICIT);
        }

        return removed.size();
    }

    private List<Entry<K, V>> terminateAll(boolean skipRunning) {
        List<Entry<K, V>> removed = new ArrayList<>();
        for (Entry<K, V> entry : indexes.values()) {
            if (removeInternal(entry, skipRunning)) {
                removed.add(entry);
            }
        }

        return removed;
    }

    @Override
    public int getPending() {
        return indexes.size();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        List<Entry<K, V>> removed = terminateAll(true);
        getEntryQueueInternal().clear();
        for (Entry<K, V> entry : removed) {
            entry.completeExceptionally(requestRemoved());
            handleListener(entry.getKey(), null, TERMINATED);
        }
    }

    @Override
    public CompletableFuture<V> enqueue(@NotNull K key) {
        if (closed.get()) {
            return Entry.<K, V>rejected(key, closed()).getCreatorFuture();
        }

        while (true) {
            Entry<K, V> existing = indexes.get(key);
            if (existing != null) {
                if (!existing.getState().isTerminal()) {
                    WaitSupport.awaitPublication(existing);
                    return Futures.view(existing.getResultFuture());
                }

                indexes.remove(key, existing);
                continue;
            }

            Entry<K, V> fresh = Entry.independent(key, Entry.State.PUBLISHING);
            if (indexes.putIfAbsent(key, fresh) == null) {
                publish(fresh);
                return fresh.getCreatorFuture();
            }
        }
    }

    private void publish(Entry<K, V> fresh) {
        Throwable failure = !closed.get()
                ? (tryReservePending() ? null : limitExceeded())
                : closed();

        if (failure != null) {
            if (!fresh.changeState(Entry.State.PUBLISHING, Entry.State.REMOVED)) {
                return;
            }

            indexes.remove(fresh.getKey(), fresh);
            fresh.completeExceptionally(failure);
            return;
        }

        if (fresh.changeState(Entry.State.PUBLISHING, Entry.State.QUEUED)) {
            enqueueEntry(fresh);
            return;
        }

        pending.decrementAndGet();
    }

    private void enqueueEntry(Entry<K, V> entry) {
        try {
            getEntryQueueInternal().push(entry);
        }
        catch (Throwable throwable) {
            queued.incrementAndGet(); // Balance QUEUED termination
            if (removeInternal(entry, false)) {
                entry.completeExceptionally(throwable);
            }

            throw queueFailed(throwable);
        }

        queued.incrementAndGet();
        entry.setLinked(true);
        if (closed.get()) {
            getEntryQueueInternal().clear();
        }
        else if (entry.getState() == Entry.State.REMOVED) {
            getEntryQueueInternal().compact();
        }
    }

    @Override
    public boolean tryComplete(@NotNull Entry<K, V> entry) {
        if (!entry.changeState(Entry.State.RUNNING, Entry.State.COMPLETED)) {
            return false;
        }

        indexes.remove(entry.getKey(), entry);
        pending.decrementAndGet();
        return true;
    }

    static final class BuilderImpl<K, V> extends AbstractBatcherBuilder<K, V, DeduplicatingBatcher<K, V>> {

        @Override
        public DeduplicatingBatcher<K, V> build(@NotNull BatchLoader<K, V> onLoader) {
            Objects.requireNonNull(onLoader, "onLoader cannot be null");
            return new DeduplicatingBatcherImpl<>(
                    getCompletionListener(), getFlushPolicy(),
                    getMaxBatchSize(), getMaxPendingRequests(),
                    getMaxRetries(), onLoader
            );
        }
    }
}
