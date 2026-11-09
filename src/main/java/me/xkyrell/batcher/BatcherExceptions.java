package me.xkyrell.batcher;

import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;

final class BatcherExceptions {

    private BatcherExceptions() {

    }

    static IllegalStateException closed() {
        return new IllegalStateException("Batcher is closed");
    }

    static RejectedExecutionException limitExceeded() {
        return new RejectedExecutionException("Maximum pending requests exceeded");
    }

    static CancellationException requestRemoved() {
        return new CancellationException("Request was removed");
    }

    static IllegalStateException queueFailed(Throwable cause) {
        return new IllegalStateException("Failed to enqueue request", cause);
    }
}
