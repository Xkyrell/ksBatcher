package me.xkyrell.batcher;

import java.util.*;

final class FlushResultImpl<K, V> implements FlushResult<K, V> {

    private static final FlushResult<?, ?> EMPTY = new FlushResultImpl<>();

    private final List<Entry<K, V>> requests;
    private final int usedKeys;
    private volatile View<K, V> view;

    private FlushResultImpl(List<Entry<K, V>> requests, int usedKeys) {
        this.requests = requests;
        this.usedKeys = usedKeys;
    }

    private FlushResultImpl() {
        this(Collections.emptyList(), 0);
    }

    private View<K, V> getOrCreateView() {
        if (view == null) {
            Map<K, V> successRequests = new LinkedHashMap<>();
            Map<K, Throwable> failedRequests = new LinkedHashMap<>();
            for (Entry<K, V> entry : requests) {
                if (entry.getState() != Entry.State.COMPLETED) {
                    continue;
                }

                entry.getResultFuture().whenComplete((value, throwable) -> {
                    if (throwable == null) {
                        successRequests.put(entry.getKey(), value);
                        return;
                    }

                    failedRequests.put(entry.getKey(), throwable);
                });
            }

            return (view = new View<>(successRequests, failedRequests));
        }
        return view;
    }

    static <K, V>FlushResult<K, V> of(
            List<Entry<K, V>> requests, int usedKeys
    ) {
        return new FlushResultImpl<>(requests, usedKeys);
    }

    @SuppressWarnings("unchecked")
    static <K, V> FlushResult<K, V> empty() {
        return (FlushResult<K, V>) EMPTY;
    }

    @Override
    public Map<K, V> getSuccessRequests() {
        return getOrCreateView().successRequests();
    }

    @Override
    public Map<K, Throwable> getFailedRequests() {
        return getOrCreateView().failedRequests();
    }

    @Override
    public int getUsedKeys() {
        return usedKeys;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        FlushResultImpl<?, ?> that = (FlushResultImpl<?, ?>) o;
        return usedKeys == that.usedKeys
                && Objects.equals(requests, that.requests)
                && Objects.equals(view, that.view);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requests, usedKeys, view);
    }

    @Override
    public String toString() {
        return "FlushResultImpl{" +
                "requests=" + requests +
                ", usedKeys=" + usedKeys +
                ", view=" + view +
                '}';
    }

    private record View<K, V>(
            Map<K, V> successRequests,
            Map<K, Throwable> failedRequests
    ) {

        private View {
            successRequests = Collections.unmodifiableMap(successRequests);
            failedRequests = Collections.unmodifiableMap(failedRequests);
        }
    }
}
