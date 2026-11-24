package io.tapdata.connector.doris.streamload;

import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.*;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.ssl.SSLContexts;

import java.net.URI;

public class HttpUtil {
    public static final int CONNECT_TIMEOUT = 5000;
    public static final int READ_TIMEOUT = 300000;
    public static final int EXPECT_CONTINUE_TIMEOUT = 30000;
    public static final int MAX_REDIRECTS = 5;
    public static final String TAG = HttpUtil.class.getSimpleName();

    private static HttpClientBuilder builder() {
        return HttpClients.custom()
                .disableAutomaticRetries()
                .setRequestExecutor(new StreamLoadRequestExecutor())
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectTimeout(CONNECT_TIMEOUT)
                        .setConnectionRequestTimeout(CONNECT_TIMEOUT)
                        .setSocketTimeout(READ_TIMEOUT)
                        .setMaxRedirects(MAX_REDIRECTS).build())
                .addInterceptorLast((HttpRequestInterceptor) HttpUtil::recordRequest)
                .addInterceptorLast((HttpResponseInterceptor) HttpUtil::recordResponse)
                .setRedirectStrategy(new StreamLoadRedirectStrategy());
    }

    private static void recordRequest(HttpRequest request, HttpContext context) {
        StreamLoadDiagnostics d = StreamLoadDiagnostics.get(context);
        if (d == null) return;
        d.lastPeer = StreamLoadDiagnostics.peer(context);
        d.requestStartedMs = d.elapsedMs();
        d.phase = "request_prepared";
        d.hops.add("request@" + d.elapsedMs() + "ms target="
                + StreamLoadDiagnostics.endpoint(String.valueOf(context.getAttribute("http.target_host")))
                + " method=" + request.getRequestLine().getMethod());
    }

    private static void recordResponse(HttpResponse response, HttpContext context) {
        StreamLoadDiagnostics d = StreamLoadDiagnostics.get(context);
        if (d == null) return;
        d.phase = "response_received";
        Header location = response.getFirstHeader("Location");
        d.hops.add("response@" + d.elapsedMs() + "ms status="
                + response.getStatusLine().getStatusCode() + " peer=" + StreamLoadDiagnostics.peer(context)
                + " location=" + StreamLoadDiagnostics.endpoint(location == null ? null : location.getValue())
                + " bodyWrites=" + d.bodyWrites + " bytesWritten=" + d.bytesWritten);
    }

    private static class StreamLoadRequestExecutor extends HttpRequestExecutor {
        StreamLoadRequestExecutor() {
            super(EXPECT_CONTINUE_TIMEOUT);
        }

        @Override
        protected HttpResponse doSendRequest(HttpRequest request, HttpClientConnection connection,
                                             HttpContext context) throws java.io.IOException, HttpException {
            // The request interceptor runs before connect; capture the actual peer here.
            StreamLoadDiagnostics d = StreamLoadDiagnostics.get(context);
            if (d != null) {
                d.lastPeer = StreamLoadDiagnostics.peer(context);
                d.phase = "send_request";
            }
            return super.doSendRequest(request, connection, context);
        }
    }

    private static class StreamLoadRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
            if (!"PUT".equalsIgnoreCase(request.getRequestLine().getMethod())) {
                return super.isRedirected(request, response, context);
            }
            int code = response.getStatusLine().getStatusCode();
            // Stream Load must never silently turn PUT into GET on a proxy's 301/302/303.
            return code == 307 || code == 308;
        }

        @Override
        public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context)
                throws ProtocolException {
            if (!"PUT".equalsIgnoreCase(request.getRequestLine().getMethod())) {
                return super.getRedirect(request, response, context);
            }
            StreamLoadDiagnostics d = StreamLoadDiagnostics.get(context);
            if (d != null) d.phase = "redirect_validation";
            URI target = resolveLocation(request, response, context, d);
            // Keep HttpClient's existing URI/routing compatibility. URI.getHost()
            // rejects authorities (e.g. underscore hostnames) that HttpClient can route.
            HttpHost source = (HttpHost) context.getAttribute("http.target_host");
            boolean httpsToHttp = source != null && "https".equalsIgnoreCase(source.getSchemeName())
                    && "http".equalsIgnoreCase(target.getScheme());
            recordRedirect(d, target, httpsToHttp);
            // Authorization is an explicit header and must accompany Doris BE uploads.
            // Reject before copying/sending rather than silently expose it over HTTP.
            if (httpsToHttp) {
                if (d != null) d.redirectFailure = "https_to_http_redirect_rejected";
                throw new ProtocolException("Refusing HTTPS to HTTP Stream Load redirect");
            }
            return org.apache.http.client.methods.RequestBuilder.copy(request).setUri(target).build();
        }

        private URI resolveLocation(HttpRequest request, HttpResponse response, HttpContext context,
                                    StreamLoadDiagnostics d) throws ProtocolException {
            try {
                return getLocationURI(request, response, context);
            } catch (org.apache.http.client.CircularRedirectException e) {
                if (d != null) d.redirectFailure = "circular_redirect";
                throw e;
            } catch (ProtocolException e) {
                if (d != null) d.redirectFailure = response.getFirstHeader("Location") == null
                        ? "missing_location" : "invalid_redirect_location";
                throw e;
            }
        }
    }

    private static void recordRedirect(StreamLoadDiagnostics d, URI target, boolean httpsToHttp) {
        if (d == null) return;
        d.hops.add("redirect@" + d.elapsedMs() + "ms target="
                + StreamLoadDiagnostics.endpoint(target.toString())
                + " javaHostMissing=" + (target.getHost() == null)
                + " hasUserInfo=" + (target.getRawAuthority() != null && target.getRawAuthority().contains("@"))
                + " hasFragment=" + (target.getRawFragment() != null)
                + " httpsToHttp=" + httpsToHttp);
    }

    public CloseableHttpClient getHttpClient() {
        return builder().build();
    }

    public static CloseableHttpClient generationHttpClient() {
        try {
            // Preserve the existing TLS policy; both factory paths share redirect handling and timeouts.
            return builder().setSSLContext(SSLContexts.custom().loadTrustMaterial(TrustAllStrategy.INSTANCE).build())
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
