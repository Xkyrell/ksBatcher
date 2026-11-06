package me.xkyrell.batcher;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Collects requests and processes them in batches.
 *
 * @param <K> the request key type
 * @param <V> the request value type
 * @since 1.0
 */
public interface Batcher<K, V> extends AutoCloseable {

    /**
     * Add a request for the specified key.
     *
     * @param key the request key
     * @return a future with the loaded value
     * @since 1.0
     */
    CompletableFuture<V> add(@NotNull K key);

    /**
     * Returns the number of pending requests.
     *
     * @return the number of pending requests
     * @since 1.0
     */
    int getPending();

    /**
     * Flushes all pending requests.
     *
     * @return a future containing the flush result
     * @since 1.0
     */
    CompletableFuture<FlushResult<K, V>> flush();

    /**
     * Flushes all pending requests.
     *
     * @return a future containing the flush result
     * @since 1.0
     */
    CompletableFuture<FlushResult<K, V>> flushImmediately();

    /**
     * Closes the batcher and terminates all pending requests.
     *
     * <p>Resources provided by the caller are not closed,
     * e.g. the configured {@link Executor}.</p>
     * @since 1.0
     */
    @Override
    void close();

    /**
     * A builder for configuring {@link Batcher} instances.
     *
     * @since 1.0
     */
    interface Builder<K, V, R extends Batcher<K, V>> {

        /**
         * Sets the listener for completed requests.
         *
         * @param onListener the completion listener
         * @return this builder
         * @since 1.0
         */
        Builder<K, V, R> completionListener(@Nullable BatchCompletionListener<K, V> onListener);

        /**
         * Sets the flush policy.
         *
         * @param policy the flush policy
         * @return this builder
         * @since 1.0
         */
        Builder<K, V, R> flushPolicy(@NotNull BatchFlushPolicy policy);

        /**
         * Sets the maximum number of requests in a batch.
         *
         * @param size the maximum batch size
         * @return this builder
         * @since 1.0
         */
        Builder<K, V, R> maxBatchSize(int size);

        /**
         * Sets the maximum number of pending requests.
         *
         * @param size the maximum number of pending requests
         * @return this builder
         * @since 1.0
         */
        Builder<K, V, R> maxPendingRequests(int size);

        /**
         * Sets the maximum number of retries for failed requests.
         *
         * @param retries the maximum number of retries
         * @return this builder
         * @since 1.0
         */
        Builder<K, V, R> maxRetries(int retries);

        /**
         * Creates a batcher using the specified loader.
         *
         * @param onLoader the batch loader
         * @return a new batcher instance
         * @since 1.0
         */
        R build(@NotNull BatchLoader<K, V> onLoader);

    }
}
