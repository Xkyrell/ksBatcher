package me.xkyrell.batcher.internal;

import org.jetbrains.annotations.ApiStatus;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

@ApiStatus.Internal
public final class Futures {

    private Futures() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static <V> CompletableFuture<V> view(CompletableFuture<V> source) {
        CompletableFuture<V> view = new CompletableFuture<>();
        source.whenComplete((value, error) -> {
            if (error == null) {
                view.complete(value);
            }
            else {
                view.completeExceptionally(unwrap(error));
            }
        });
        return view;
    }

    public static <V> CompletableFuture<V> toFuture(CompletionStage<V> stage) {
        if (stage instanceof CompletableFuture<V> future) {
            return future;
        }

        CompletableFuture<V> mirror = new CompletableFuture<>();
        try {
            stage.whenComplete((value, error) -> {
                if (error == null) {
                    mirror.complete(value);
                }
                else {
                    mirror.completeExceptionally(error);
                }
            });
        }
        catch (Throwable thrown) {
            mirror.completeExceptionally(thrown);
        }

        return mirror;
    }

    public static Throwable unwrap(Throwable throwable) {
        while (throwable instanceof CompletionException && throwable.getCause() != null) {
            throwable = throwable.getCause();
        }
        return throwable;
    }
}
