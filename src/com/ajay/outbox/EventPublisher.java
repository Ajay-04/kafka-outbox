package com.ajay.outbox;

/**
 * Publishes one outbox event to the broker.
 *
 * Contract: return normally only after the broker acked. If the event reached
 * the broker but the ack was lost, throwing is still correct — the relay will
 * redeliver and the idempotent consumer dedupes. Never swallow a failure.
 */
public interface EventPublisher {
    void publish(OutboxEvent event) throws PublishException;
}
