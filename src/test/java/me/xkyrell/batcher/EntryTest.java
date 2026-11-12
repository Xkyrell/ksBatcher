package me.xkyrell.batcher;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EntryTest {

    @Test
    void testIndependentEntry() {
        Entry<String, Integer> entry = Entry.independent("a", Entry.State.QUEUED);
        assertNotSame(entry.getCreatorFuture(), entry.getResultFuture());

        entry.complete(1);
        assertEquals(1, entry.getCreatorFuture().join());
        assertEquals(1, entry.getResultFuture().join());
    }

    @Test
    void testCancelledCreator() {
        Entry<String, Integer> entry = Entry.independent("a", Entry.State.QUEUED);
        entry.getCreatorFuture().cancel(false);
        assertFalse(entry.getResultFuture().isDone());

        entry.complete(1);
        assertEquals(1, entry.getResultFuture().join());
        assertTrue(entry.getCreatorFuture().isCancelled());
    }

    @Test
    void testSharedEntry() {
        Entry<String, Integer> entry = Entry.shared("a", Entry.State.QUEUED);
        assertSame(entry.getCreatorFuture(), entry.getResultFuture());

        entry.complete(1);
        assertEquals(1, entry.getCreatorFuture().join());
    }

    @Test
    void testStateTransition() {
        Entry<String, Integer> entry = Entry.shared("a", Entry.State.PUBLISHING);

        assertFalse(entry.changeState(Entry.State.QUEUED, Entry.State.RUNNING));
        assertEquals(Entry.State.PUBLISHING, entry.getState());
        assertTrue(entry.changeState(Entry.State.PUBLISHING, Entry.State.QUEUED));
        assertTrue(entry.changeState(Entry.State.QUEUED, Entry.State.RUNNING));
        assertTrue(entry.changeState(Entry.State.RUNNING, Entry.State.COMPLETED));
        assertEquals(Entry.State.COMPLETED, entry.getState());
    }

    @Test
    void testTerminateIgnoresState() {
        Entry<String, Integer> entry = Entry.shared("a", Entry.State.COMPLETED);
        assertNull(entry.terminate(false));
        assertEquals(Entry.State.COMPLETED, entry.getState());
    }

    @Test
    void testTerminateSkip() {
        Entry<String, Integer> running = Entry.shared("a", Entry.State.RUNNING);
        Entry<String, Integer> queued = Entry.shared("b", Entry.State.QUEUED);
        assertNull(running.terminate(true));
        assertEquals(Entry.State.RUNNING, running.getState());
        assertEquals(Entry.State.QUEUED, queued.terminate(true));
        assertEquals(Entry.State.REMOVED, queued.getState());
    }
}
