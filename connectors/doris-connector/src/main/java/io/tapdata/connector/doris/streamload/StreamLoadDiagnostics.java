package io.tapdata.connector.doris.streamload;

import org.apache.http.HttpInetConnection;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Per-execution metadata only: never retain headers, credentials, queries or row data. */
final class StreamLoadDiagnostics {
    static final String KEY = StreamLoadDiagnostics.class.getName();
    final long started = System.nanoTime();
    final List<String> hops = new ArrayList<>();
    long requestStartedMs;
    String lastPeer = "unknown";
    String initialEndpoint = "unknown";
    long batchBytes;
    int bodyWrites;
    long bytesWritten;
    long firstBodyMs = -1;

    static StreamLoadDiagnostics get(HttpContext context) {
        return (StreamLoadDiagnostics) context.getAttribute(KEY);
    }

    long elapsedMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }

    // Logging only the origin also removes user-info, signed query parameters and fragments.
    static String endpoint(String value) {
        if (value == null) return "missing";
        try {
            URI uri = URI.create(value);
            if (uri.getHost() == null) return uri.isAbsolute() ? "invalid" : "relative";
            return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), null, null, null).toASCIIString();
        } catch (Exception ignored) {
            return "invalid";
        }
    }

    static String peer(HttpContext context) {
        try {
            Object connection = context.getAttribute(HttpCoreContext.HTTP_CONNECTION);
            if (connection instanceof HttpInetConnection) {
                HttpInetConnection inet = (HttpInetConnection) connection;
                return String.valueOf(inet.getRemoteAddress()) + ":" + inet.getRemotePort();
            }
        } catch (RuntimeException ignored) {
            // A Connection: close response may already have detached the managed connection.
        }
        StreamLoadDiagnostics d = get(context);
        return d == null ? "unknown" : d.lastPeer;
    }

    @Override
    public String toString() {
        return "initialEndpoint=" + initialEndpoint + ", batchBytes=" + batchBytes + ", repeatable=true, elapsedMs=" + elapsedMs() + ", expectWaitMs=" + HttpUtil.EXPECT_CONTINUE_TIMEOUT
                + ", connectTimeoutMs=" + HttpUtil.CONNECT_TIMEOUT + ", readTimeoutMs=" + HttpUtil.READ_TIMEOUT
                + ", maxRedirects=" + HttpUtil.MAX_REDIRECTS + ", bodyWrites=" + bodyWrites
                + ", bytesWritten=" + bytesWritten + ", firstBodyMs=" + firstBodyMs + ", hops=" + hops;
    }
}
