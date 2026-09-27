package com.ajay.outbox;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory OutboxStore for the demo and tests. Claim semantics mirror
 * {@code SELECT ... FOR UPDATE SKIP LOCKED}: rows already claimed by another
 * relay instance are skipped, never double-claimed. All state transitions are
 * synchronized, so N relay threads can poll concurrently.
 */
public final class InMemoryOutboxStore implements OutboxStore {
    private static final class Row {
        final OutboxEvent event;
        boolean claimed;
        boolean published;
        Row(OutboxEvent event) { this.event = event; }
    }

    private final Map<String, Row> rows = new LinkedHashMap<>();

    @Override
    public synchronized void append(OutboxEvent event) {
        rows.put(event.id(), new Row(event));
    }

    @Override
    public synchronized List<OutboxEvent> claimUnpublished(int limit) {
        List<OutboxEvent> batch = new ArrayList<>();
        for (Row row : rows.values()) {
            if (batch.size() >= limit) break;
            if (!row.published && !row.claimed) {
                row.claimed = true;
                batch.add(row.event);
            }
        }
        return batch;
    }

    @Override
    public synchronized void markPublished(String eventId) {
        Row row = rows.get(eventId);
        if (row != null && row.claimed) {
            row.published = true;
            row.claimed = false;
        }
    }

    @Override
    public synchronized void releaseClaim(String eventId) {
        Row row = rows.get(eventId);
        if (row != null && !row.published) {
            row.claimed = false;
        }
    }

    @Override
    public synchronized int unpublishedCount() {
        int n = 0;
        for (Row row : rows.values()) {
            if (!row.published) n++;
        }
        return n;
    }
}
