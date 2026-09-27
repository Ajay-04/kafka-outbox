package com.ajay.outbox;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable domain event staged in the outbox table.
 * The producer's business transaction writes the row; the relay publishes it.
 */
public final class OutboxEvent {
    private final String id;
    private final String aggregateType;
    private final String aggregateId;
    private final String type;
    private final String payload;
    private final Instant createdAt;

    public OutboxEvent(String aggregateType, String aggregateId, String type, String payload) {
        this(UUID.randomUUID().toString(), aggregateType, aggregateId, type, payload, Instant.now());
    }

    public OutboxEvent(String id, String aggregateType, String aggregateId,
                       String type, String payload, Instant createdAt) {
        this.id = Objects.requireNonNull(id, "id");
        this.aggregateType = Objects.requireNonNull(aggregateType, "aggregateType");
        this.aggregateId = Objects.requireNonNull(aggregateId, "aggregateId");
        this.type = Objects.requireNonNull(type, "type");
        this.payload = Objects.requireNonNull(payload, "payload");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
    }

    public String id() { return id; }
    public String aggregateType() { return aggregateType; }
    public String aggregateId() { return aggregateId; }
    public String type() { return type; }
    public String payload() { return payload; }
    public Instant createdAt() { return createdAt; }
    /** First 8 chars of the id, for readable demo output. */
    public String shortId() { return id.substring(0, 8); }
}
