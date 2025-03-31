package io.tapdata.connector.starrocks.streamload.exception;

/**
 * @Author dayun
 * @Date 7/14/22
 */
public class StarrocksRuntimeException extends RuntimeException {
    public StarrocksRuntimeException() {
        super();
    }

    public StarrocksRuntimeException(String message) {
        super(message);
    }

    public StarrocksRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public StarrocksRuntimeException(Throwable cause) {
        super(cause);
    }

    protected StarrocksRuntimeException(String message, Throwable cause,
                                    boolean enableSuppression,
                                    boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
