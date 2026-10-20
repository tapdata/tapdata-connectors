package io.tapdata.connector.paimon.service;

import java.math.BigDecimal;
import java.sql.SQLTimeoutException;
import java.util.Collections;
import java.util.Map;
import java.util.function.LongSupplier;

/** Per-command limits; timeout is cooperative at catalog/scan/batch/row/result boundaries. */
final class PaimonQueryLimits {
    final long maxScanRows;
    final int maxGroups;
    final int maxResultRows;
    final boolean allowFullScan;
    private final long started;
    private final long timeoutNanos;
    private final LongSupplier clock;

    PaimonQueryLimits(Map<String, Object> params) { this(params, System::nanoTime); }

    PaimonQueryLimits(Map<String, Object> params, LongSupplier clock) {
        Map<String, Object> values = params == null ? Collections.emptyMap() : params;
        maxScanRows = positive(values, "maxScanRows", 10_000_000, Long.MAX_VALUE);
        maxGroups = (int) positive(values, "maxGroups", 100_000, Integer.MAX_VALUE);
        maxResultRows = (int) positive(values, "maxResultRows", 1000, Integer.MAX_VALUE);
        long seconds = positive(values, "queryTimeoutSeconds", 300, Integer.MAX_VALUE);
        timeoutNanos = seconds * 1_000_000_000L;
        Object fullScan = values.get("allowFullScan");
        if (fullScan != null && !(fullScan instanceof Boolean)) {
            throw new IllegalArgumentException("Parameter 'allowFullScan' must be a boolean");
        }
        allowFullScan = Boolean.TRUE.equals(fullScan);
        this.clock = clock;
        started = clock.getAsLong();
    }

    void check() throws SQLTimeoutException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) { throw new InterruptedException("Paimon executeQuery interrupted"); }
        if (clock.getAsLong() - started >= timeoutNanos) {
            throw new SQLTimeoutException("Paimon executeQuery exceeded queryTimeoutSeconds; no partial result returned");
        }
    }

    long elapsedMillis() { return (clock.getAsLong() - started) / 1_000_000; }

    IllegalStateException exceeded(String limit, long value) {
        return new IllegalStateException("Paimon executeQuery exceeded " + limit + "=" + value
                + "; narrow the WHERE range or increase params." + limit + "; no partial result returned");
    }

    private static long positive(Map<String, Object> params, String key, long defaultValue, long max) {
        Object raw = params.get(key);
        if (raw == null) { return defaultValue; }
        try {
            long value = new BigDecimal(raw.toString()).longValueExact();
            if (value <= 0 || value > max) { throw new ArithmeticException("out of range"); }
            return value;
        } catch (NumberFormatException | ArithmeticException failure) {
            throw new IllegalArgumentException("Parameter '" + key + "' must be an integer between 1 and " + max, failure);
        }
    }
}
