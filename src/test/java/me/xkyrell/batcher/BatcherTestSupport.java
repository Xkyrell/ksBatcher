package me.xkyrell.batcher;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

final class BatcherTestSupport {

    private BatcherTestSupport() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    static Throwable failure(CompletableFuture<?> future) {
        try {
            future.join();
        }
        catch (CompletionException | CancellationException ex) {
            return ex instanceof CompletionException ? ex.getCause() : ex;
        }

        throw new AssertionError();
    }

    static int getQueued(Batcher<?, ?> batcher) {
        return ((AbstractBatcher<?, ?>) batcher).queued.get();
    }

    static int getRunning(Batcher<?, ?> batcher) {
        return batcher.getPending() - getQueued(batcher);
    }
}
