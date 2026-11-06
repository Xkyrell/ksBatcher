package me.xkyrell.batcher;

import me.xkyrell.batcher.internal.Futures;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

final class Entry<K, V> {

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<Entry> STATE =
            AtomicIntegerFieldUpdater.newUpdater(Entry.class, "state");

    private final K key;
    private final CompletableFuture<V> creator;
    private final CompletableFuture<V> result;
    private volatile int state;
    private volatile boolean linked;

    private Entry(K key, CompletableFuture<V> creator, CompletableFuture<V> result, int state) {
        this.key = key;
        this.creator = creator;
        this.result = result;
        this.state = state;
    }

    static <K, V> Entry<K, V> independent(K key, State state) {
        return new Entry<>(key, new CompletableFuture<>(), new CompletableFuture<>(), state.ordinal());
    }

    static <K, V> Entry<K, V> shared(K key, State state) {
        CompletableFuture<V> future = new CompletableFuture<>();
        return new Entry<>(key, future, future, state.ordinal());
    }

    static <K, V> Entry<K, V> rejected(K key, Throwable cause) {
        Entry<K, V> entry = shared(key, State.REMOVED);
        entry.completeExceptionally(cause);
        return entry;
    }

    void complete(V value) {
        result.complete(value);
        if (creator != result) {
            creator.complete(value);
        }
    }

    void completeExceptionally(Throwable throwable) {
        Throwable cause = Futures.unwrap(throwable);
        result.completeExceptionally(cause);
        if (creator != result) {
            creator.completeExceptionally(cause);
        }
    }

    State terminate(boolean skipRunning) {
        while (true) {
            State current = getState();
            if (current.isTerminal() || (skipRunning && current == State.RUNNING)) {
                return null;
            }

            if (changeState(current, State.REMOVED)) {
                return current;
            }
        }
    }

    boolean changeState(State from, State to) {
        return STATE.compareAndSet(this, from.ordinal(), to.ordinal());
    }

    K getKey() {
        return key;
    }

    CompletableFuture<V> getCreatorFuture() {
        return creator;
    }

    CompletableFuture<V> getResultFuture() {
        return result;
    }

    State getState() {
        return State.VALUES[state];
    }

    boolean isLinked() {
        return linked;
    }

    void setLinked(boolean linked) {
        this.linked = linked;
    }

    enum State {

        PUBLISHING,
        QUEUED,
        RUNNING,
        COMPLETED,
        REMOVED;

        private static final State[] VALUES = values();

        boolean isTerminal() {
            return this == COMPLETED || this == REMOVED;
        }
    }
}
