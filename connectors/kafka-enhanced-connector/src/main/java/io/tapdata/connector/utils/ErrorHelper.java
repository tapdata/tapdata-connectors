package io.tapdata.connector.utils;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/8 22:15 Create
 */
public class ErrorHelper {

    private final AtomicReference<Exception> exception = new AtomicReference<>(null);

    public void closeQuietly(AutoCloseable closeable) {
        if (null == closeable) return;

        try {
            closeable.close();
        } catch (Exception e) {
            if (!exception.compareAndSet(null, e)) {
                exception.get().addSuppressed(e);
            }
        }
    }

    public ErrorHelper closeQuietly(AutoCloseable... closeableList) {
        if (null != closeableList) {
            for (AutoCloseable closeable : closeableList) {
                closeQuietly(closeable);
            }
        }
        return this;
    }

    public Exception get() {
        return exception.get();
    }

    public void check() throws Exception {
        if (null != exception.get()) {
            throw exception.get();
        }
    }

    public static ErrorHelper create(AutoCloseable... closeable) {
        return new ErrorHelper().closeQuietly(closeable);
    }

    public static void closeAndCheck(AutoCloseable... closeable) throws Exception {
        create(closeable).check();
    }

    public static Exception closeAndGet(AutoCloseable... closeable) {
        return create(closeable).get();
    }

    public static void closeWithNotNull(AutoCloseable closeable) throws Exception {
        if (null != closeable) {
            closeable.close();
        }
    }
}
