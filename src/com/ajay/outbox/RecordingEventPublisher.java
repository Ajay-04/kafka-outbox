package com.ajay.outbox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * In-memory EventPublisher for the demo. Delivers to a downstream consumer
 * and records every publish call, with failure injection for the harness:
 * crash between broker ack and markPublished, the nastiest real-world case.
 */
public final class RecordingEventPublisher implements EventPublisher {
    private final Consumer<OutboxEvent> downstream;
    private final List<String> publishCalls = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger attempts = new AtomicInteger();
    private volatile int crashOnAttempt = -1;

    public RecordingEventPublisher(Consumer<OutboxEvent> downstream) {
        this.downstream = downstream;
    }

    /**
     * Arms a one-shot crash: the publish call with this 1-based attempt number
     * delivers the event, then throws — exactly like a relay dying after the
     * broker ack but before markPublished commits.
     */
    public void crashOnAttempt(int n) {
        this.crashOnAttempt = n;
    }

    public void clearCrash() {
        this.crashOnAttempt = -1;
    }

    @Override
    public void publish(OutboxEvent event) throws PublishException {
        int n = attempts.incrementAndGet();
        publishCalls.add(event.id());
        downstream.accept(event); // the event reached the broker / consumer
        if (n == crashOnAttempt) {
            crashOnAttempt = -1;
            throw new PublishException(
                    "simulated crash: ack received, markPublished never ran");
        }
    }

    public int publishCalls() {
        return publishCalls.size();
    }

    public long uniquePublished() {
        return new HashSet<>(publishCalls).size();
    }
}
