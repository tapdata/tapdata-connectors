package io.tapdata.connector.paimon.exception;

/** Existing dynamic-bucket data violates global primary-key uniqueness and needs operator repair. */
public final class PaimonDynamicBucketPollutedException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    /**
     * Literal emitted by Paimon's {@code GlobalIndexAssigner} when the dynamic-bucket global index
     * already contains the primary key being inserted. This is a fragile string contract tied to
     * Paimon 1.3.x ({@code GlobalIndexAssigner} source); if Paimon changes the message, detection
     * silently breaks. Centralized here so an upstream version bump touches exactly one place.
     */
    private static final String DUPLICATE_KEY_MESSAGE = "data contains duplicates";

    public PaimonDynamicBucketPollutedException(String tableKey, Throwable cause) {
        super(
                "Paimon dynamic-bucket table " + tableKey
                        + " already contains duplicate primary keys. Stop all writers, deduplicate "
                        + "or rebuild the table from the source of truth, then restart the task.",
                cause);
    }

    /**
     * Returns true if any exception in the cause chain mentions the Paimon dynamic-bucket
     * duplicate-key marker. Walks {@link Throwable#getCause()} links so wrapped/chained failures
     * (e.g. from {@code ReflectionException} or {@code RuntimeException} proxies) are still
     * detected. Case-sensitive {@code contains}, matching the upstream literal exactly.
     */
    public static boolean causeChainContains(Throwable error, String text) {
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(text)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * If {@code cause} (or any exception in its cause chain) carries the Paimon dynamic-bucket
     * duplicate-key marker, wrap it in a {@link PaimonDynamicBucketPollutedException} that names
     * the offending {@code tableKey}; otherwise return {@code cause} unchanged. Callers that need
     * an additional precondition (e.g. {@code bucketMode == KEY_DYNAMIC}) should guard before
     * calling, since this method only inspects the error text.
     *
     * <p>Accepts a broad {@link Throwable} input (matching {@code catch (Exception e)} and kernel
     * error shapes) but returns {@link Exception} so callers can assign the result to existing
     * {@code Exception failure} locals without a cast. This is type-safe because the wrap branch
     * returns a {@code PaimonDynamicBucketPollutedException} (an {@code IllegalStateException}) and
     * the pass-through branch returns the original {@code cause} verbatim — and every existing
     * caller catches {@code Exception}, so {@code cause} is in practice always an {@code Exception}.
     */
    public static Exception wrapIfPolluted(String tableKey, Throwable cause) {
        if (causeChainContains(cause, DUPLICATE_KEY_MESSAGE)) {
            return new PaimonDynamicBucketPollutedException(tableKey, cause);
        }
        return (cause instanceof Exception)
                ? (Exception) cause
                : new RuntimeException(cause);
    }
}
