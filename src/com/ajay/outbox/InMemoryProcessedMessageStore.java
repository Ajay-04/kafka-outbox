package com.ajay.outbox;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Thread-safe in-memory ProcessedMessageStore for the demo and tests. */
public final class InMemoryProcessedMessageStore implements ProcessedMessageStore {
    private final Set<String> seen = ConcurrentHashMap.newKeySet();

    @Override
    public boolean tryMark(String messageId) {
        return seen.add(messageId);
    }

    public int size() {
        return seen.size();
    }
}
