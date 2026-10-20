package io.tapdata.common;

import java.math.BigDecimal;
import java.util.Map;

final class SqlCommandParams {

    private SqlCommandParams() {
    }

    static int getInteger(Map<String, Object> params, String name, int defaultValue) {
        Object value = params.get(name);
        if (value == null) {
            return defaultValue;
        }
        try {
            int parsed = new BigDecimal(value.toString()).intValueExact();
            if (parsed <= 0) { throw new ArithmeticException("must be positive"); }
            return parsed;
        } catch (NumberFormatException | ArithmeticException e) {
            throw new IllegalArgumentException("Parameter '" + name + "' must be a positive whole number between 1 and " + Integer.MAX_VALUE + "; got: " + value, e);
        }
    }

    static int getQueryTimeoutSeconds(Map<String, Object> params) {
        Object value = params.get("queryTimeoutSeconds");
        if (value == null) {
            return 0;
        }
        final long seconds;
        try {
            seconds = new BigDecimal(value.toString()).longValueExact();
        } catch (NumberFormatException | ArithmeticException e) {
            throw new IllegalArgumentException("Parameter 'queryTimeoutSeconds' must be a whole number between 0 and 2147483647 (0 disables the timeout)", e);
        }
        if (seconds < 0) {
            throw new IllegalArgumentException("Parameter 'queryTimeoutSeconds' must be non-negative; 0 disables the timeout");
        }
        if (seconds > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Parameter 'queryTimeoutSeconds' must not exceed " + Integer.MAX_VALUE);
        }
        return (int) seconds;
    }
}
