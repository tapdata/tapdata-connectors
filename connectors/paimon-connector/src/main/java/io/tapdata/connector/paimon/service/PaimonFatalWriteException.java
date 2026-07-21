package io.tapdata.connector.paimon.service;

/**
 * A deterministic row or target-schema violation which cannot succeed by replaying the same
 * source event. The service propagates this exception without converting it to a retryable PDK
 * error, while fencing the whole service even when validation fails before writer ingress starts.
 */
final class PaimonFatalWriteException extends IllegalArgumentException {

    PaimonFatalWriteException(String message) {
        super(message);
    }
}
