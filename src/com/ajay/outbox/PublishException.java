package com.ajay.outbox;

/** The broker did not ack. The relay treats the row as unpublished and retries. */
public final class PublishException extends Exception {
    public PublishException(String message) { super(message); }
    public PublishException(String message, Throwable cause) { super(message, cause); }
}
