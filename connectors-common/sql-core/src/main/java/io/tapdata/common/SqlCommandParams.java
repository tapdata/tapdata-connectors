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
            return new BigDecimal(value.toString()).intValueExact();
        } catch (NumberFormatException | ArithmeticException e) {
            throw new IllegalArgumentException("Parameter '" + name + "' must be a 32-bit integer", e);
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
            throw new IllegalArgumentException("Parameter 'queryTimeoutSeconds' must be an integer", e);
        }
        if (seconds <= 0) {
            return 0;
        }
        if (seconds > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Parameter 'queryTimeoutSeconds' must not exceed " + Integer.MAX_VALUE);
        }
        return (int) seconds;
    }
}
