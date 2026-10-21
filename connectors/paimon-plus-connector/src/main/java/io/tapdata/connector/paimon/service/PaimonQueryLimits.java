package io.tapdata.connector.paimon.service;

import java.math.BigDecimal;
import java.sql.SQLTimeoutException;
import java.util.Collections;
import java.util.Map;
import java.util.function.LongSupplier;

/** Per-command limits; timeout is cooperative at catalog/scan/batch/row/result boundaries. */
final class PaimonQueryLimits {
    private final Map<String, Object> params;
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
        this.params = values;
        maxScanRows = positive(values, "maxScanRows", 10_000_000, Long.MAX_VALUE);
        maxGroups = (int) positive(values, "maxGroups", 100_000, Integer.MAX_VALUE);
        maxResultRows = (int) positive(values, "maxResultRows", Integer.MAX_VALUE, Integer.MAX_VALUE);
        long seconds = nonNegative(values, "queryTimeoutSeconds", 0, Integer.MAX_VALUE);
        timeoutNanos = seconds * 1_000_000_000L;
        Object fullScan = values.get("allowFullScan");
        allowFullScan = booleanValue(fullScan);
        this.clock = clock;
        started = clock.getAsLong();
    }

    void check() throws SQLTimeoutException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) { throw new InterruptedException("Paimon executeQuery interrupted"); }
        if (timeoutNanos > 0 && clock.getAsLong() - started >= timeoutNanos) {
            throw new SQLTimeoutException("Paimon executeQuery exceeded queryTimeoutSeconds=" + timeoutNanos / 1_000_000_000L
                    + "; increase params.queryTimeoutSeconds or set it to 0 to disable the cooperative deadline"
                    + "; blocked storage I/O cannot be interrupted; command failed; discard any previously delivered batches");
        }
    }

    /** Keep historical SELECT * and COUNT defaults without resetting the command clock. */
    PaimonQueryLimits forLegacyQuery() { return new PaimonQueryLimits(this); }

    private PaimonQueryLimits(PaimonQueryLimits original) {
        params = original.params;
        maxScanRows = positive(params, "maxScanRows", Long.MAX_VALUE, Long.MAX_VALUE);
        maxGroups = (int) positive(params, "maxGroups", Integer.MAX_VALUE, Integer.MAX_VALUE);
        maxResultRows = (int) positive(params, "maxResultRows", Integer.MAX_VALUE, Integer.MAX_VALUE);
        allowFullScan = original.allowFullScan;
        started = original.started;
        timeoutNanos = original.timeoutNanos;
        clock = original.clock;
    }

    long elapsedMillis() { return (clock.getAsLong() - started) / 1_000_000; }

    IllegalStateException exceeded(String limit, long value) {
        return new IllegalStateException("Paimon executeQuery exceeded " + limit + "=" + value
                + "; narrow the WHERE range or increase params." + limit + "; command failed; discard any previously delivered batches");
    }

    private static long nonNegative(Map<String, Object> params, String key, long defaultValue, long max) {
        Object raw = params.get(key);
        if (raw == null) { return defaultValue; }
        try {
            long value = new BigDecimal(raw.toString()).longValueExact();
            if (value < 0 || value > max) { throw new ArithmeticException("out of range"); }
            return value;
        } catch (NumberFormatException | ArithmeticException failure) {
            throw new IllegalArgumentException("Parameter '" + key + "' must be a whole number between 0 and " + max, failure);
        }
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

    private static boolean booleanValue(Object raw) {
        if (raw == null) { return false; }
        if (raw instanceof Boolean) { return (Boolean) raw; }
        if ("true".equalsIgnoreCase(raw.toString())) { return true; }
        if ("false".equalsIgnoreCase(raw.toString())) { return false; }
        throw new IllegalArgumentException("Parameter 'allowFullScan' must be true or false");
    }
}
