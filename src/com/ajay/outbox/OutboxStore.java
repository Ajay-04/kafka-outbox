package com.ajay.outbox;

import java.util.List;

/**
 * The outbox table. The production implementation is a JDBC class over
 * Postgres; see schema.sql for the DDL and the claim query
 * (SELECT ... FOR UPDATE SKIP LOCKED). This interface keeps the relay
 * decoupled from the storage engine.
 */
public interface OutboxStore {
    /** Insert one unpublished row. Called inside the business transaction. */
    void append(OutboxEvent event);

    /**
     * Atomically claim up to {@code limit} unpublished rows for this relay
     * instance. Concurrent relays must never receive the same row.
     */
    List<OutboxEvent> claimUnpublished(int limit);

    /** Mark a row published. Called only after the broker acked. */
    void markPublished(String eventId);

    /** Release a claim so the row becomes claimable again (relay crash path). */
    void releaseClaim(String eventId);

    int unpublishedCount();
}
