package io.tapdata.connector.paimon.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the static helpers on {@link PaimonDynamicBucketPollutedException}
 * ({@code causeChainContains} / {@code wrapIfPolluted}). These cover the cause-chain
 * walking, literal matching, and pass-through semantics that were previously
 * duplicated inline in {@code PaimonTableWriteContextFactory} and
 * {@code PaimonDynamicBucketPreflight}.
 */
class PaimonDynamicBucketPollutedExceptionTest {

    private static final String MARKER = "data contains duplicates";

    @Test
    void causeChainContainsMustMatchDirectMessage() {
        // Direct hit on the top-level exception's message.
        assertTrue(PaimonDynamicBucketPollutedException.causeChainContains(
                new RuntimeException("operation failed: data contains duplicates at bucket 3"),
                MARKER));
    }

    @Test
    void causeChainContainsMustWalkCauseChain() {
        // The marker is buried two levels deep; the top-level message is unrelated.
        Throwable leaf = new IllegalStateException("data contains duplicates");
        Throwable middle = new RuntimeException("writer closed", leaf);
        Throwable top = new RuntimeException("commit failed", middle);
        assertTrue(PaimonDynamicBucketPollutedException.causeChainContains(top, MARKER));
    }

    @Test
    void causeChainContainsMustBeCaseSensitive() {
        // The Paimon kernel literal is lowercase "data contains duplicates"; a case-shifted
        // variant must NOT match. This locks in case-sensitive contains so a future "fix"
        // to toLowerCase() does not silently widen detection.
        assertFalse(PaimonDynamicBucketPollutedException.causeChainContains(
                new RuntimeException("Data Contains Duplicates"), MARKER));
    }

    @Test
    void causeChainContainsMustRejectNullMessageAndMissingText() {
        // null message on every level must not NPE; missing text must return false.
        Throwable nullMessage = new NullPointerException();
        assertFalse(PaimonDynamicBucketPollutedException.causeChainContains(nullMessage, MARKER));
        assertFalse(PaimonDynamicBucketPollutedException.causeChainContains(
                new RuntimeException("unrelated error"), MARKER));
    }

    @Test
    void causeChainContainsMustHandleNullError() {
        // Defensive: a null error input must return false rather than NPE.
        assertFalse(PaimonDynamicBucketPollutedException.causeChainContains(null, MARKER));
    }

    @Test
    void wrapIfPollutedMustReturnExceptionWhenMarkerMatches() {
        Throwable cause = new RuntimeException("data contains duplicates");
        Throwable result = PaimonDynamicBucketPollutedException.wrapIfPolluted("orders", cause);

        assertTrue(result instanceof PaimonDynamicBucketPollutedException);
        // Original cause must be preserved as the wrap's cause (no information loss).
        assertSame(cause, result.getCause());
        // The tableKey must appear in the operator-facing message.
        assertTrue(result.getMessage().contains("orders"));
    }

    @Test
    void wrapIfPollutedMustReturnOriginalCauseWhenMarkerAbsent() {
        // Pass-through: unrelated errors come back as the exact same instance, unwrapped,
        // so callers' downstream error handling (e.g. fatal-vs-retryable classification) is
        // unaffected.
        Throwable cause = new IllegalStateException("network timeout");
        Throwable result = PaimonDynamicBucketPollutedException.wrapIfPolluted("orders", cause);

        assertSame(cause, result);
    }

    @Test
    void wrapIfPollutedMustDetectMarkerInNestedCause() {
        // Mirrors the production failure shape: Paimon wraps the duplicate-key error
        // inside a chain; wrapIfPolluted must still surface the polluted variant.
        Throwable leaf = new IllegalStateException("data contains duplicates");
        Throwable wrapped = new RuntimeException("batch write aborted", leaf);
        Throwable result = PaimonDynamicBucketPollutedException.wrapIfPolluted("orders", wrapped);

        assertTrue(result instanceof PaimonDynamicBucketPollutedException);
        // The original wrapped throwable is the direct cause (not the leaf), preserving
        // the full chain for diagnostics.
        assertSame(wrapped, result.getCause());
    }
}
