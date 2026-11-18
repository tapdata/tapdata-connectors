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
                .setRequestExecutor(new HttpRequestExecutor(EXPECT_CONTINUE_TIMEOUT) {
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
                })
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectTimeout(CONNECT_TIMEOUT)
                        .setConnectionRequestTimeout(CONNECT_TIMEOUT)
                        .setSocketTimeout(READ_TIMEOUT)
                        .setMaxRedirects(MAX_REDIRECTS).build())
                .addInterceptorLast((HttpRequestInterceptor) (request, context) -> {
                    StreamLoadDiagnostics d = StreamLoadDiagnostics.get(context);
                    if (d != null) d.lastPeer = StreamLoadDiagnostics.peer(context);
                    if (d != null) {
                        d.requestStartedMs = d.elapsedMs();
                        d.phase = "request_prepared";
                    }
                    if (d != null) d.hops.add("request@" + d.elapsedMs() + "ms target="
                            + StreamLoadDiagnostics.endpoint(String.valueOf(context.getAttribute("http.target_host")))
                            + " method=" + request.getRequestLine().getMethod());
                })
                .addInterceptorLast((HttpResponseInterceptor) (response, context) -> {
                    StreamLoadDiagnostics d = StreamLoadDiagnostics.get(context);
                    if (d != null) d.phase = "response_received";
                    Header location = response.getFirstHeader("Location");
                    if (d != null) d.hops.add("response@" + d.elapsedMs() + "ms status="
                            + response.getStatusLine().getStatusCode() + " peer=" + StreamLoadDiagnostics.peer(context)
                            + " location=" + StreamLoadDiagnostics.endpoint(location == null ? null : location.getValue())
                            + " bodyWrites=" + d.bodyWrites + " bytesWritten=" + d.bytesWritten);
                })
                .setRedirectStrategy(new DefaultRedirectStrategy() {
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
                        URI target;
                        try {
                            target = getLocationURI(request, response, context);
                        } catch (org.apache.http.client.CircularRedirectException e) {
                            if (d != null) d.redirectFailure = "circular_redirect";
                            throw e;
                        } catch (ProtocolException e) {
                            if (d != null) d.redirectFailure = response.getFirstHeader("Location") == null
                                    ? "missing_location" : "invalid_redirect_location";
                            throw e;
                        }
                        // Keep HttpClient's existing URI/routing compatibility. URI.getHost()
                        // rejects authorities (e.g. underscore hostnames) that HttpClient can route.
                        // Diagnostics must observe unusual targets, never introduce a new veto.
                        if (d != null) {
                            HttpHost source = (HttpHost) context.getAttribute("http.target_host");
                            d.hops.add("redirect@" + d.elapsedMs() + "ms target="
                                    + StreamLoadDiagnostics.endpoint(target.toString())
                                    + " javaHostMissing=" + (target.getHost() == null)
                                    + " hasUserInfo=" + (target.getRawAuthority() != null && target.getRawAuthority().contains("@"))
                                    + " hasFragment=" + (target.getRawFragment() != null)
                                    + " httpsToHttp=" + (source != null && "https".equalsIgnoreCase(source.getSchemeName())
                                    && "http".equalsIgnoreCase(target.getScheme())));
                        }
                        return org.apache.http.client.methods.RequestBuilder.copy(request).setUri(target).build();
                    }
                });
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
