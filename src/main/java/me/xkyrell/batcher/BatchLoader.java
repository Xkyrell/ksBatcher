package me.xkyrell.batcher;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Loads values for a batch of keys
 *
 * @param <K> the request key type
 * @param <V> the request value type
 * @since 1.0
 */
@FunctionalInterface
public interface BatchLoader<K, V> {

    /**
     * Loads values for the specified keys.
     *
     * @param keys the keys to load
     * @return a stage containing the loaded values
     * @since 1.0
     */
    CompletionStage<Map<K, V>> load(@NotNull List<K> keys);

    /**
     * Wraps a synchronous loader function.
     *
     * @param onLoader the loader function
     * @param <K> the request key type
     * @param <V> the request value type
     * @return a loader
     */
    static <K, V> BatchLoader<K, V> wrap(@NotNull Function<List<K>, Map<K, V>> onLoader) {
        Objects.requireNonNull(onLoader, "onLoader cannot be null");
        return keys -> CompletableFuture.completedFuture(onLoader.apply(keys));
    }
}
