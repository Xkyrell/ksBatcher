package me.xkyrell.batcher;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

final class BatchRecordingListener implements BatchCompletionListener<String, Integer> {

    private final List<String> records = new CopyOnWriteArrayList<>();

    @Override
    public void onCompletion(String key, Integer value, Cause cause) {
        records.add(key + ":" + cause);
    }

    List<String> getRecords() {
        return records;
    }
}
