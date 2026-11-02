package me.xkyrell.batcher;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Listener called when a request completes in the {@link Batcher}.
 *
 * @param <K> the request key type
 * @param <V> the request value type
 * @since 1.0
 */
@FunctionalInterface
public interface BatchCompletionListener<K, V> {

    /**
     * Notifies that a request has completed.
     *
     * @param key   the request key
     * @param value the request value
     * @param cause the removal cause
     * @since 1.0
     */
    void onCompletion(@NotNull K key, @Nullable V value, @NotNull Cause cause);

    /**
     * The reason a request completed.
     *
     * @since 1.0
     */
    enum Cause {

        /**
         * The request completed successfully.
         *
         * @since 1.0
         */
        SUCCESS,

        /**
         * The request failed because the loader could not load the requested key.
         *
         * @since 1.0
         */
        FAILED,

        /**
         * The request was dropped by the loader.
         *
         * @since 1.0
         */
        DROPPED,

        /**
         * The request was explicitly removed by the caller.
         *
         * @since 1.0
         */
        EXPLICIT,

        /**
         * The request was terminated because the batcher was closed.
         *
         * @since 1.0
         */
        TERMINATED;

        /**
         * Returns whether the request completed successfully.
         *
         * @return {@code true} if the request was successful
         * @since 1.0
         */
        public boolean isSuccess() {
            return this == SUCCESS;
        }
    }
}
