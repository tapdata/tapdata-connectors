package io.tapdata.connector.paimon.service;

/** Existing dynamic-bucket data violates global primary-key uniqueness and needs operator repair. */
final class PaimonDynamicBucketPollutedException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    PaimonDynamicBucketPollutedException(String tableKey, Throwable cause) {
        super(
                "Paimon dynamic-bucket table " + tableKey
                        + " already contains duplicate primary keys. Stop all writers, deduplicate "
                        + "or rebuild the table from the source of truth, then restart the task.",
                cause);
    }
}
