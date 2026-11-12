package me.xkyrell.batcher;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import java.util.Objects;

abstract class AbstractBatcherBuilder<K, V, R extends Batcher<K, V>>
        implements Batcher.Builder<K, V, R> {

    private static final BatchFlushPolicy DEFAULT_FLUSH_POLICY = BatchFlushPolicy.LAZY;
    private static final int DEFAULT_MAX_BATCH_SIZE = 100;
    private static final int DEFAULT_PENDING_REQUESTS = 0;
    private static final int DEFAULT_MAX_RETRIES = 0;

    private BatchCompletionListener<K, V> onListener;
    private BatchFlushPolicy flushPolicy;
    private int maxBatchSize;
    private int maxPendingRequests;
    private int maxRetries;

    protected AbstractBatcherBuilder() {
        flushPolicy = DEFAULT_FLUSH_POLICY;
        maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
        maxPendingRequests = DEFAULT_PENDING_REQUESTS;
        maxRetries = DEFAULT_MAX_RETRIES;
    }

    @Override
    public Batcher.Builder<K, V, R> completionListener(
            @Nullable BatchCompletionListener<K, V> onListener
    ) {
        this.onListener = onListener;
        return this;
    }

    @Override
    public Batcher.Builder<K, V, R> flushPolicy(@NotNull BatchFlushPolicy flushPolicy) {
        this.flushPolicy = flushPolicy;
        return this;
    }

    @Override
    public Batcher.Builder<K, V, R> maxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    @Override
    public Batcher.Builder<K, V, R> maxPendingRequests(int maxPendingRequests) {
        this.maxPendingRequests = maxPendingRequests;
        return this;
    }

    @Override
    public Batcher.Builder<K, V, R> maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    protected final BatchFlushPolicy getFlushPolicy() {
        return Objects.requireNonNull(flushPolicy, "flushPolicy cannot be null");
    }

    protected final int getMaxBatchSize() {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        return maxBatchSize;
    }

    protected final int getMaxPendingRequests() {
        if (maxPendingRequests < 0) {
            throw new IllegalArgumentException("maxPendingRequests must not be negative");
        }
        return maxPendingRequests;
    }

    protected final int getMaxRetries() {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be negative");
        }
        return maxRetries;
    }

    protected final BatchCompletionListener<K, V> getCompletionListener() {
        return onListener;
    }
}
