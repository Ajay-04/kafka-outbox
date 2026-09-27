package com.ajay.outbox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumer side of the pattern. At-least-once delivery from the relay plus
 * this dedupe is what gives the system effectively-once processing:
 * redeliveries are absorbed by the processed-message table, never re-applied.
 */
public final class IdempotentConsumer {
    private final ProcessedMessageStore processed;
    private final List<String> ledger = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger duplicatesSkipped = new AtomicInteger();

    public IdempotentConsumer(ProcessedMessageStore processed) {
        this.processed = processed;
    }

    /** Applies the event exactly once per message id, however often redelivered. */
    public void handle(OutboxEvent event) {
        if (processed.tryMark(event.id())) {
            // The business side effect goes here (update read model, send email...).
            ledger.add(event.type() + " aggregate=" + event.aggregateId()
                    + " id=" + event.shortId());
        } else {
            duplicatesSkipped.incrementAndGet();
        }
    }

    /** Ordered record of every applied side effect. */
    public List<String> ledger() {
        return List.copyOf(ledger);
    }

    public int appliedCount() {
        return ledger.size();
    }

    public int duplicatesSkipped() {
        return duplicatesSkipped.get();
    }
}
