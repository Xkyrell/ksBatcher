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
     * Checks whether a request for the specified key is pending.
     *
     * @param key the request key
     * @return {@code true} if the request is pending
     * @since 1.0
     */
    boolean contains(@NotNull K key);

    /**
     * Removes the pending request for the specified key.
     *
     * @param key the request key
     * @return {@code true} if the request was removed
     * @since 1.0
     */
    boolean remove(@NotNull K key);

    /**
     * Removes all pending requests.
     *
     * @return the number of removed requests
     * @since 1.0
     */
    int removeAll();

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
     * @return a future completed after processing
     * @since 1.0
     */
    CompletableFuture<Void> flush();

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
     * A builder for configuring a {@link Batcher}.
     *
     * @since 1.0
     */
    interface Builder<K, V> {

        /**
         * Sets the listener for removed requests.
         *
         * @param onListener the removal listener
         * @return this builder
         * @since 1.0
         */
        Builder<K, V> removeListener(@Nullable BatchRemovalListener<K, V> onListener);

        /**
         * Sets the flush policy.
         *
         * @param policy the flush policy
         * @return this builder
         * @since 1.0
         */
        Builder<K, V> flushPolicy(@NotNull BatchFlushPolicy policy);

        /**
         * Sets the executor used for batch processing.
         *
         * @param executor the executor
         * @return this builder
         * @since 1.0
         */
        Builder<K, V> executor(@NotNull Executor executor);

        /**
         * Sets the maximum number of requests in a batch.
         *
         * @param size the maximum batch size
         * @return this builder
         * @since 1.0
         */
        Builder<K, V> maxBatchSize(int size);

        /**
         * Sets the maximum number of pending requests.
         *
         * @param size the maximum number of pending requests
         * @return this builder
         * @since 1.0
         */
        Builder<K, V> maxPendingRequests(int size);

        /**
         * Sets the maximum number of retries for failed requests.
         *
         * @param retries the maximum number of retries
         * @return this builder
         * @since 1.0
         */
        Builder<K, V> maxRetries(int retries);

        /**
         * Creates a batcher using the specified loader.
         *
         * @param onLoader the batch loader
         * @return a new batcher
         * @since 1.0
         */
        Batcher<K, V> build(@NotNull BatchLoader<K, V> onLoader);

    }
}
