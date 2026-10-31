package me.xkyrell.batcher;

import java.util.Map;

/**
 * Represents the result of a batch flush.
 *
 * @param <K> the request key type
 * @param <V> the request value type
 * @since 1.0
 */
public interface FlushResult<K, V> {

    /**
     * Returns the successfully loaded requests.
     *
     * @return the successful requests
     * @since 1.0
     */
    Map<K, V> getSuccessRequests();

    /**
     * Returns the failed requests and their causes.
     *
     * @return the failed requests
     * @since 1.0
     */
    Map<K, Throwable> getFailedRequests();

    /**
     * Returns the number of keys used during the flush.
     *
     * @return the number of used keys
     * @since 1.0
     */
    int getUsedKeys();

    /**
     * Checks whether the result contains no requests.
     *
     * @return {@code true} if there are no successful or failed requests
     * @since 1.0
     */
    default boolean isEmpty() {
        return getSuccessRequests().isEmpty() && getFailedRequests().isEmpty();
    }

    /**
     * Checks whether all requests were processed successfully.
     *
     * @return {@code true} if there are no failed requests
     * @since 1.0
     */
    default boolean isSuccessful() {
        return getFailedRequests().isEmpty();
    }
}
