package com.ajay.outbox;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Polls the outbox and publishes. The ordering guarantee that matters:
 * a row is marked published only after the broker acked. A crash anywhere
 * before that leaves the row unpublished, so the next poll redelivers it
 * and the idempotent consumer absorbs the duplicate.
 *
 * Multiple relay instances may poll concurrently: {@link OutboxStore#claimUnpublished}
 * hands each row to exactly one instance (SKIP LOCKED semantics).
 */
public final class OutboxRelay {
    private final String name;
    private final OutboxStore store;
    private final EventPublisher publisher;
    private final AtomicInteger claimedTotal = new AtomicInteger();
    private final AtomicInteger publishedTotal = new AtomicInteger();

    public OutboxRelay(String name, OutboxStore store, EventPublisher publisher) {
        this.name = name;
        this.store = store;
        this.publisher = publisher;
    }

    /**
     * One poll cycle. Returns the number of events marked published.
     * On a publish failure the unprocessed remainder of the batch is released
     * so this or another instance retries it on the next poll.
     */
    public int pollOnce(int batchSize) {
        List<OutboxEvent> batch = store.claimUnpublished(batchSize);
        claimedTotal.addAndGet(batch.size());
        int ok = 0;
        for (int i = 0; i < batch.size(); i++) {
            OutboxEvent event = batch.get(i);
            try {
                publisher.publish(event);
                store.markPublished(event.id());
                publishedTotal.incrementAndGet();
                ok++;
            } catch (PublishException e) {
                for (int j = i; j < batch.size(); j++) {
                    store.releaseClaim(batch.get(j).id());
                }
                break;
            }
        }
        return ok;
    }

    public String name() { return name; }
    public int claimedTotal() { return claimedTotal.get(); }
    public int publishedTotal() { return publishedTotal.get(); }
}
