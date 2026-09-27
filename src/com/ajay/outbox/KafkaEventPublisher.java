package com.ajay.outbox;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Production publisher: sends each outbox event to Kafka and blocks for the
 * broker ack before returning, so the relay marks rows published only after
 * Kafka durably has them.
 *
 * <p>Needs {@code kafka-clients} on the classpath and a reachable broker:
 * <pre>
 *   javac -cp kafka-clients-3.7.0.jar -d out $(find src -name '*.java')
 * </pre>
 * See docker-compose.yml for a local Kafka + Postgres to run against.
 */
public final class KafkaEventPublisher implements EventPublisher, AutoCloseable {
    private final KafkaProducer<String, String> producer;
    private final long ackTimeoutSeconds;

    public KafkaEventPublisher(Properties producerProps) {
        this(producerProps, 10);
    }

    public KafkaEventPublisher(Properties producerProps, long ackTimeoutSeconds) {
        this.producer = new KafkaProducer<>(producerProps);
        this.ackTimeoutSeconds = ackTimeoutSeconds;
    }

    @Override
    public void publish(OutboxEvent event) throws PublishException {
        // One topic per event type keeps consumer routing trivial. The event id
        // travels in a header so the consumer can dedupe without parsing payload.
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "outbox." + event.type(), event.aggregateId(), event.payload());
        record.headers().add("event-id", event.id().getBytes(StandardCharsets.UTF_8));
        record.headers().add("event-type", event.type().getBytes(StandardCharsets.UTF_8));
        try {
            producer.send(record).get(ackTimeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new PublishException(
                    "kafka publish failed for event " + event.shortId(), e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
