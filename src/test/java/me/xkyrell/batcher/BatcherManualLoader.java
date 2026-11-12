package me.xkyrell.batcher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

final class BatcherManualLoader implements BatchLoader<String, Integer> {

    private final List<List<String>> calls = new CopyOnWriteArrayList<>();
    private final List<CompletableFuture<Map<String, Integer>>> stages = new CopyOnWriteArrayList<>();

    @Override
    public CompletionStage<Map<String, Integer>> load(List<String> keys) {
        CompletableFuture<Map<String, Integer>> stage = new CompletableFuture<>();
        calls.add(keys);
        stages.add(stage);
        return stage;
    }

    List<List<String>> getCalls() {
        return calls;
    }

    List<CompletableFuture<Map<String, Integer>>> getStages() {
        return stages;
    }

    void complete(int call, Map<String, Integer> values) {
        stages.get(call).complete(values);
    }

    void completeExceptionally(int call, Throwable error) {
        stages.get(call).completeExceptionally(error);
    }
}
