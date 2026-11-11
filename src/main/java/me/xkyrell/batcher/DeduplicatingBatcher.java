package me.xkyrell.batcher;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public interface DeduplicatingBatcher<K, V> extends Batcher<K, V> {

    /**
     * Creates a new batcher builder with request deduplication by key
     * and support for managing pending requests.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @return a new batcher builder
     * @since 1.0
     */
    @Contract(value = " -> new", pure = true)
    static <K, V> Builder<K, V, DeduplicatingBatcher<K, V>> builder() {
        return new DeduplicatingBatcherImpl.BuilderImpl<>();
    }

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

}
