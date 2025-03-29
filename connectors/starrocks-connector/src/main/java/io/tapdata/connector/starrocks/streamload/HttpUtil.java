package io.tapdata.connector.starrocks.streamload;

import io.tapdata.entity.logger.TapLogger;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.*;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.Args;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.util.concurrent.TimeUnit;

/**
 * @Author dayun
 * @Date 7/14/22
 */
public class HttpUtil {
    public static final int CONNECT_TIMEOUT = 5000;
    public static final int READ_TIMEOUT = 300000;
    public static final String TAG = HttpUtil.class.getSimpleName();
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });

    public CloseableHttpClient getHttpClient() {
        return httpClientBuilder.build();
    }
    public static CloseableHttpClient generationHttpClient() {
        try {
            TrustAllStrategy instance = TrustAllStrategy.INSTANCE;
            SSLContext sslContext = SSLContexts.custom()
                    .loadTrustMaterial(instance)
                    .build();
            HostnameVerifier hostnameVerifier = NoopHostnameVerifier.INSTANCE;
            HttpClientBuilder custom = HttpClients.custom();
            custom.setSSLContext(sslContext);
            custom.setSSLHostnameVerifier(hostnameVerifier);
            custom.setRedirectStrategy(new LaxRedirectStrategy() {
                @Override
                public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
                    Args.notNull(request, "HTTP request");
                    Args.notNull(response, "HTTP response");
                    int statusCode = response.getStatusLine().getStatusCode();
                    String method = request.getRequestLine().getMethod();
                    Header locationHeader = response.getFirstHeader("location");
                    switch (statusCode) {
                        case 301:
                        case 308:
                            return this.isRedirectable(method);
                        case 302:
                            return this.isRedirectable(method) && locationHeader != null;
                        case 303:
                        case 307:
                            return true;
                        case 304:
                        case 305:
                        case 306:
                        default:
                            return false;
                    }
                }
            });
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(CONNECT_TIMEOUT)
                    .setSocketTimeout(READ_TIMEOUT)
                    .build();
            custom.setDefaultRequestConfig(requestConfig);
            CloseableHttpClient httpClient = custom
                    .build();
            return httpClient;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
