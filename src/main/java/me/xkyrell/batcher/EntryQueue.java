package me.xkyrell.batcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;

import static me.xkyrell.batcher.Entry.State.QUEUED;
import static me.xkyrell.batcher.Entry.State.RUNNING;

final class EntryQueue<K, V> {

    private static final int MIN_COMPACTION_SIZE = 64;

    private final AtomicReference<Node<K, V>> head = new AtomicReference<>();
    private final AtomicBoolean compacting = new AtomicBoolean();
    private final IntSupplier queued;

    EntryQueue(IntSupplier queued) {
        this.queued = queued;
    }

    void push(Entry<K, V> entry) {
        Node<K, V> node = new Node<>(entry, null);
        while (true) {
            Node<K, V> top = head.get();
            node.link(top);
            if (head.compareAndSet(top, node)) {
                return;
            }
        }
    }

    List<Entry<K, V>> detach(int size) {
        List<Entry<K, V>> entries = new ArrayList<>(size);
        for (Node<K, V> cursor = head.getAndSet(null); cursor != null; cursor = cursor.previous) {
            Entry<K, V> entry = cursor.entry;
            if (entry.getState() == QUEUED && entry.changeState(QUEUED, RUNNING)) {
                entries.add(entry);
            }
        }

        Collections.reverse(entries); // The stack is newest first
        return entries;
    }

    void clear() {
        head.set(null);
    }

    void compact() {
        while (shouldCompact() && compacting.compareAndSet(false, true)) {
            try {
                compactInternal(head.get());
            }
            finally {
                compacting.set(false);
            }
        }
    }

    private boolean shouldCompact() {
        Node<K, V> top = head.get();
        if (top == null || top.size < MIN_COMPACTION_SIZE) {
            return false;
        }

        return top.size > 2L * Math.max(0, queued.getAsInt());
    }

    private void compactInternal(Node<K, V> node) {
        List<Entry<K, V>> entries = new ArrayList<>();
        for (Node<K, V> cursor = node; cursor != null; cursor = cursor.previous) {
            Entry<K, V > entry = cursor.entry;
            if (entry.getState() == QUEUED) {
                entries.add(entry);
            }
        }

        Node<K, V> newHead = null;
        for (int i = entries.size() - 1; i >= 0; i--) {
            newHead = new Node<>(entries.get(i), newHead);
        }

        while (true) {
            Node<K, V> current = head.get();
            List<Entry<K, V>> newerEntries = new ArrayList<>();
            Node<K, V> cursor = current;
            while (cursor != null && cursor != node) {
                newerEntries.add(cursor.entry);
                cursor = cursor.previous;
            }

            if (cursor == null) return; // A flush took the stack

            Node<K, V> replacement = newHead;
            for (int i = newerEntries.size() - 1; i >= 0; i--) {
                replacement = new Node<>(newerEntries.get(i), replacement);
            }

            if (head.compareAndSet(current, replacement)) {
                return;
            }
        }
    }

    private static final class Node<K, V> {

        private final Entry<K, V> entry;
        private Node<K, V> previous;
        private int size;

        private Node(Entry<K, V> entry, Node<K, V> previous) {
            this.entry = entry;
            link(previous);
        }

        private void link(Node<K, V> below) {
            this.previous = below;
            this.size = below != null ? below.size + 1 : 1;
        }
    }
}

