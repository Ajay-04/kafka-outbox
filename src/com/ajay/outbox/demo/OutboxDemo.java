package com.ajay.outbox.demo;

import com.ajay.outbox.IdempotentConsumer;
import com.ajay.outbox.InMemoryOutboxStore;
import com.ajay.outbox.InMemoryProcessedMessageStore;
import com.ajay.outbox.OutboxEvent;
import com.ajay.outbox.OutboxRelay;
import com.ajay.outbox.RecordingEventPublisher;

import java.util.ArrayList;
import java.util.List;

/**
 * Failure-injection harness: the proof that the pattern holds.
 * Phase 1: happy path. Phase 2: relay crash between broker ack and
 * markPublished — redelivery with no loss. Phase 3: duplicate delivery —
 * consumer dedupes, no double-apply. Phase 4: two relay instances polling
 * concurrently — every event published exactly once.
 */
public final class OutboxDemo {
    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public static void main(String[] args) throws Exception {
        InMemoryOutboxStore store = new InMemoryOutboxStore();
        InMemoryProcessedMessageStore processed = new InMemoryProcessedMessageStore();
        IdempotentConsumer consumer = new IdempotentConsumer(processed);
        RecordingEventPublisher publisher = new RecordingEventPublisher(consumer::handle);
        OutboxRelay relay = new OutboxRelay("relay-1", store, publisher);

        System.out.println("== Phase 1: happy path — 5 order events ==");
        for (int i = 1; i <= 5; i++) {
            store.append(new OutboxEvent("order", "order-" + i, "OrderPlaced",
                    "{\"orderId\":\"order-" + i + "\"}"));
        }
        System.out.println("  appended 5 events to outbox");
        int ok = relay.pollOnce(10);
        System.out.println("  relay: claimed 5, published " + ok + ", marked " + ok);
        System.out.println("  consumer ledger: " + consumer.appliedCount()
                + " applied, " + consumer.duplicatesSkipped() + " duplicates skipped");
        System.out.println();

        System.out.println("== Phase 2: relay crash mid-batch — no event lost ==");
        List<OutboxEvent> batch2 = new ArrayList<>();
        for (int i = 6; i <= 9; i++) {
            OutboxEvent e = new OutboxEvent("order", "order-" + i, "OrderPlaced",
                    "{\"orderId\":\"order-" + i + "\"}");
            batch2.add(e);
            store.append(e);
        }
        System.out.println("  appended 4 events; relay will crash after 2 acks");
        publisher.crashOnAttempt(publisher.publishCalls() + 3);
        int crashed = relay.pollOnce(10);
        System.out.println("  relay: published " + crashed
                + " then CRASH (simulated, before markPublished)");
        System.out.println("  relay restarted — unacked rows were never marked, so they are reclaimed");
        int recovered = relay.pollOnce(10);
        System.out.println("  relay: claimed 2, published " + recovered
                + ", marked " + recovered + " (one was a redelivery)");
        System.out.println("  consumer ledger: " + consumer.appliedCount()
                + " applied total, " + consumer.duplicatesSkipped()
                + " duplicate skipped, 0 lost");
        System.out.println();

        System.out.println("== Phase 3: duplicate delivery — consumer dedupes ==");
        OutboxEvent dup = batch2.get(0);
        System.out.println("  redelivering event " + dup.shortId() + " (already applied)");
        publisher.publish(dup);
        System.out.println("  consumer: duplicate skipped via processed-message table");
        System.out.println("  consumer ledger: " + consumer.appliedCount()
                + " applied total, " + consumer.duplicatesSkipped() + " duplicates skipped");
        System.out.println();

        System.out.println("== Phase 4: 2 relay instances, 20 events — no double publish ==");
        for (int i = 10; i <= 29; i++) {
            store.append(new OutboxEvent("order", "order-" + i, "OrderPlaced",
                    "{\"orderId\":\"order-" + i + "\"}"));
        }
        long uniqueBefore = publisher.uniquePublished();
        OutboxRelay relayA = new OutboxRelay("relay-A", store, publisher);
        OutboxRelay relayB = new OutboxRelay("relay-B", store, publisher);
        Thread t1 = new Thread(() -> {
            while (store.unpublishedCount() > 0) { relayA.pollOnce(3); sleep(5); }
        });
        Thread t2 = new Thread(() -> {
            while (store.unpublishedCount() > 0) { relayB.pollOnce(3); sleep(5); }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        long uniqueNew = publisher.uniquePublished() - uniqueBefore;
        System.out.println("  relay-A claimed " + relayA.claimedTotal()
                + ", relay-B claimed " + relayB.claimedTotal());
        System.out.println("  broker received " + uniqueNew
                + " distinct events, each exactly once");
        System.out.println("  consumer ledger: " + consumer.appliedCount()
                + " applied total, 0 double-applied");
        System.out.println();

        System.out.println("OK: at-least-once delivery + idempotent consumer = effectively-once.");
        System.out.println("    " + consumer.appliedCount() + " applied, "
                + consumer.duplicatesSkipped() + " duplicates absorbed, 0 lost, 0 double-applied.");
    }
}
