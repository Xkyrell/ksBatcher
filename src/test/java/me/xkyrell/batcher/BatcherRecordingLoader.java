package me.xkyrell.batcher;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

final class BatcherRecordingLoader implements BatchLoader<String, Integer> {

    private final List<List<String>> calls = new CopyOnWriteArrayList<>();

    @Override
    public CompletionStage<Map<String, Integer>> load(List<String> keys) {
        calls.add(new ArrayList<>(keys));
        Map<String, Integer> result = new LinkedHashMap<>();
        keys.forEach(key -> result.put(key, key.charAt(0) - 'a' + 1));
        return CompletableFuture.completedFuture(result);
    }

    List<List<String>> getCalls() {
        return calls;
    }
}