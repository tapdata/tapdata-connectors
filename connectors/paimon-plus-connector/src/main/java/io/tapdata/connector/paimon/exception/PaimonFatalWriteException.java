package io.tapdata.connector.paimon.exception;

/**
 * A deterministic CDC value or target-schema violation which cannot succeed by replaying the same
 * source event. When observed by the write service it is recorded as a sticky failure and fences
 * subsequent writes; connector codecs may also raise it before service ingress.
 */
public final class PaimonFatalWriteException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public PaimonFatalWriteException(String message) {
        super(message);
    }
}
