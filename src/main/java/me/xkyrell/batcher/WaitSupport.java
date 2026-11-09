package me.xkyrell.batcher;

import java.util.concurrent.locks.LockSupport;

final class WaitSupport {

    private static final int SPIN_LIMIT = 64;
    private static final int YIELD_LIMIT = 1_000;
    private static final long PARK_DURATION = 50_000L;

    private WaitSupport() {

    }

    static void awaitPublication(Entry<?, ?> entry) {
        for (int round = 0; !entry.isLinked(); round++) {
            var state = entry.getState();
            if (state == Entry.State.RUNNING || state.isTerminal()) {
                return;
            }

            if (round < SPIN_LIMIT) {
                Thread.onSpinWait();
            }
            else if (round < SPIN_LIMIT + YIELD_LIMIT) {
                Thread.yield();
            }
            else {
                LockSupport.parkNanos(PARK_DURATION);
            }
        }
    }
}
