package me.xkyrell.batcher;

import me.xkyrell.batcher.internal.Futures;
import org.jetbrains.annotations.Nullable;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

final class BatchRunner<K, V> {

    private static final Logger log = Logger.getLogger(BatchRunner.class.getName());

    private final BatchLoader<K, V> loader;
    private final RequestHandler<K, V> onHandler;
    private final BooleanSupplier onClosed;

    BatchRunner(BatchLoader<K, V> loader, RequestHandler<K, V> onHandler, BooleanSupplier onClosed) {
        this.loader = loader;
        this.onHandler = onHandler;
        this.onClosed = onClosed;
    }

    CompletableFuture<FlushResult<K, V>> runBatches(
            List<Entry<K, V>> entries, int maxBatchSize, int maxRetries
    ) {
        if (entries.isEmpty()) {
            return CompletableFuture.completedFuture(FlushResultImpl.empty());
        }

        if (entries.size() <= maxBatchSize) {
            return new BatchExecution(entries, maxRetries).execute()
                    .thenApply(usedKeys -> FlushResultImpl.of(entries, usedKeys));
        }

        List<CompletableFuture<Integer>> batches = new ArrayList<>();
        for (int from = 0; from < entries.size(); from += maxBatchSize) {
            int to = Math.min(entries.size(), from + maxBatchSize);
            batches.add(new BatchExecution(entries.subList(from, to), maxRetries).execute());
        }

        return CompletableFuture.allOf(batches.toArray(new CompletableFuture[0]))
                .thenApply(__ -> createAggregatedResult(entries, batches));
    }

    private FlushResult<K, V> createAggregatedResult(
            List<Entry<K, V>> entries,
            List<CompletableFuture<Integer>> batches
    ) {
        int usedKeys = 0;
        for (CompletableFuture<Integer> batch : batches) {
            usedKeys += batch.join();
        }

        return FlushResultImpl.of(entries, usedKeys);
    }

    private final class BatchExecution {

        private final List<Entry<K, V>> batchEntries;
        private final int maxRetries;
        private final CompletableFuture<Integer> done = new CompletableFuture<>();
        private int usedKeys;

        private BatchExecution(List<Entry<K, V>> batchEntries, int maxRetries) {
            this.batchEntries = batchEntries;
            this.maxRetries = maxRetries;
        }

        CompletableFuture<Integer> execute() {
            executeAttempt(batchEntries);
            return done;
        }

        private void executeAttempt(List<Entry<K, V>> pendingEntries) {
            try {
                List<Entry<K, V>> activeEntries = getActiveEntries(pendingEntries);
                while (!activeEntries.isEmpty()) {
                    usedKeys++;
                    CompletableFuture<Map<K, V>> loading = load(activeEntries);
                    if (!loading.isDone()) {
                        List<Entry<K, V>> requestedEntries = activeEntries;
                        loading.whenComplete((__, ___) -> {
                            executeAttempt(resolveAttempt(requestedEntries, loading));
                        });
                        return;
                    }

                    activeEntries = getActiveEntries(resolveAttempt(activeEntries, loading));
                }

                done.complete(usedKeys);
            }
            catch (Throwable throwable) {
                log.log(Level.SEVERE, "Unexpected error while running a batch", throwable);

                handleFailure(batchEntries, throwable);
                done.complete(usedKeys);
            }
        }

        private List<Entry<K, V>> getActiveEntries(List<Entry<K, V>> candidates) {
            int size = candidates.size();
            int first = 0;
            while (first < size && candidates.get(first).getState() == Entry.State.RUNNING) {
                first++;
            }

            if (first == size) return candidates;

            List<Entry<K, V>> active = new ArrayList<>(size - 1);
            active.addAll(candidates.subList(0, first));
            for (int index = first + 1; index < size; index++) {
                if (candidates.get(index).getState() == Entry.State.RUNNING) {
                    active.add(candidates.get(index));
                }
            }

            return active;
        }

        private CompletableFuture<Map<K, V>> load(List<Entry<K, V>> activeEntries) {
            List<K> keys = new ArrayList<>(activeEntries.size());
            for (Entry<K, V> entry : activeEntries) {
                keys.add(entry.getKey());
            }

            try {
                return Futures.toFuture(Objects.requireNonNull(
                        loader.load(keys), "loader cannot return null"
                ));
            }
            catch (Throwable throwable) {
                return CompletableFuture.failedFuture(throwable);
            }
        }

        private List<Entry<K, V>> resolveAttempt(
                List<Entry<K, V>> activeEntries, CompletableFuture<Map<K, V>> loading
        ) {
            try {
                Map<K, V> values;
                try {
                    values = loading.join();
                }
                catch (CompletionException ex) {
                    return resolveRetryIfPossible(activeEntries,
                            ex.getCause() != null ? ex.getCause() : ex
                    );
                }
                catch (CancellationException ex) {
                    return resolveRetryIfPossible(activeEntries, ex);
                }

                if (values == null) {
                    return resolveRetryIfPossible(activeEntries,
                            new NullPointerException("loader results cannot be null")
                    );
                }

                acceptResults(activeEntries, values);
                return Collections.emptyList();
            }
            catch (Throwable throwable) {
                log.log(Level.SEVERE, "Unexpected error while settling a batch", throwable);
                handleFailure(activeEntries, throwable);
                return Collections.emptyList();
            }
        }

        private void acceptResults(List<Entry<K, V>> entries, Map<K, V> values) {
            for (Entry<K, V> entry : entries) {
                if (entry.getState() == Entry.State.RUNNING) {
                    continue;
                }

                if (values.containsKey(entry.getKey())) {
                    onHandler.accept(entry, values.get(entry.getKey()), null);
                    return;
                }

                onHandler.accept(entry, null, new KeyNotLoadedException(entry.getKey()));
            }
        }

        private List<Entry<K, V>> resolveRetryIfPossible(
                List<Entry<K, V>> activeEntries, Throwable cause
        ) {
            if (usedKeys > maxRetries || onClosed.getAsBoolean()) {
                for (Entry<K, V> entry : activeEntries) {
                    if (entry.getState() == Entry.State.RUNNING) {
                        onHandler.accept(entry, null, cause);
                    }
                }
                return Collections.emptyList();
            }

            List<Entry<K, V>> retryEntries = new ArrayList<>(activeEntries.size());
            for (Entry<K, V> entry : activeEntries) {
                if (entry.getState() == Entry.State.RUNNING) {
                    retryEntries.add(entry);
                }
            }

            return retryEntries;
        }

        private void handleFailure(List<Entry<K, V>> entries, Throwable cause) {
            for (Entry<K, V> entry : entries) {
                if (entry.getState() == Entry.State.RUNNING) {
                    onHandler.accept(entry, null, cause);
                }
            }
        }
    }

    @FunctionalInterface
    interface RequestHandler<K, V> {

        void accept(Entry<K, V> entry, @Nullable V value, @Nullable Throwable cause);

    }
}
