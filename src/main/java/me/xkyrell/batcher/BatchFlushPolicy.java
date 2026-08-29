package me.xkyrell.batcher;

/**
 * Specifies when pending requests are flushed.
 *
 * @since 1.0
 */
public enum BatchFlushPolicy {

    /**
     * Requests are flushed when the batch reaches its configured limit.
     *
     * @since 1.0
     */
    LAZY,

    /**
     * Requests are flushed only when {@code flush()} is called explicitly.
     *
     * @since 1.0
     */
    MANUAL

}
