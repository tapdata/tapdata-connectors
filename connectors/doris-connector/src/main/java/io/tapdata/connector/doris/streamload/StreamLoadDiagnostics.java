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
    String phase = "prepare";
    String redirectFailure = "none";
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
            org.apache.http.HttpHost host = org.apache.http.client.utils.URIUtils.extractHost(uri);
            if (host == null) return uri.isAbsolute() ? "invalid" : "relative";
            return host.toURI();
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

    // Exception messages from HttpClient may contain full Location URLs, credentials or row data.
    // Report the complete bounded type chain, but expose only recognized protocol messages.
    String failureSummary(Throwable failure) {
        StringBuilder chain = new StringBuilder();
        java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
        for (int i = 0; failure != null && i < 12 && seen.add(failure); i++, failure = failure.getCause()) {
            if (chain.length() > 0) chain.append(" -> ");
            chain.append(failure.getClass().getSimpleName());
            String message = failure.getMessage();
            if (failure instanceof org.apache.http.client.CircularRedirectException) {
                chain.append("[circular_redirect]");
            } else if (failure instanceof org.apache.http.client.RedirectException) {
                chain.append(message != null && message.startsWith("Maximum redirects (")
                        ? "[maximum_redirects_exceeded]" : "[redirect_rejected]");
            } else if (failure instanceof org.apache.http.ProtocolException) {
                if ("Content-Length header already present".equals(message)) chain.append("[duplicate_content_length]");
                else if ("Transfer-encoding header already present".equals(message)) chain.append("[duplicate_transfer_encoding]");
                else if (message != null && message.startsWith("Received redirect response") && message.endsWith("missing location header")) chain.append("[missing_location]");
                else if (failure.getCause() instanceof java.net.URISyntaxException) chain.append("[invalid_redirect_uri]");
                else chain.append("[protocol_error]");
            }
        }
        return "phase=" + phase + ", redirectFailure=" + redirectFailure + ", causeChain=" + chain;
    }

    @Override
    public String toString() {
        return "initialEndpoint=" + initialEndpoint + ", batchBytes=" + batchBytes + ", repeatable=true, elapsedMs=" + elapsedMs() + ", expectWaitMs=" + HttpUtil.EXPECT_CONTINUE_TIMEOUT
                + ", connectTimeoutMs=" + HttpUtil.CONNECT_TIMEOUT + ", readTimeoutMs=" + HttpUtil.READ_TIMEOUT
                + ", maxRedirects=" + HttpUtil.MAX_REDIRECTS + ", bodyWrites=" + bodyWrites
                + ", bytesWritten=" + bytesWritten + ", firstBodyMs=" + firstBodyMs + ", hops=" + hops;
    }
}
