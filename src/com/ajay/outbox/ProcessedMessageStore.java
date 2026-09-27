package com.ajay.outbox;

/**
 * The consumer-side dedupe table (see schema.sql: processed_messages).
 * Same idea as an idempotency key: the message id is the dedupe key.
 */
public interface ProcessedMessageStore {
    /**
     * Atomically records the message id. Returns true if this is the first
     * time the id is seen; false if it was already processed.
     */
    boolean tryMark(String messageId);
}
