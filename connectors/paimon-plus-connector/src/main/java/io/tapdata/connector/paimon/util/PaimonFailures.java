package io.tapdata.connector.paimon.util;

/** STOP 多层观察同一失败时，保留首因并按异常对象身份去重。 */
public final class PaimonFailures {
    private PaimonFailures() {}

    // Paimon 1.3.2 ExceptionUtils.firstOrSuppressed 保留首因；这里额外去重重复观察的次因。
    // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-common/src/main/java/org/apache/paimon/utils/ExceptionUtils.java#L284-L293
    public static Throwable append(Throwable first, Throwable next) {
        if (first == null) { return next; }
        if (next == null || first == next) { return first; }
        synchronized (first) {
            for (Throwable suppressed : first.getSuppressed()) {
                if (suppressed == next) { return first; }
            }
            first.addSuppressed(next);
        }
        return first;
    }
}
