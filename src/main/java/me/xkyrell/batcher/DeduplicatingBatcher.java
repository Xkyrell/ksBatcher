package me.xkyrell.batcher;

import org.jetbrains.annotations.NotNull;

public interface DeduplicatingBatcher<K, V> extends Batcher<K, V> {

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
