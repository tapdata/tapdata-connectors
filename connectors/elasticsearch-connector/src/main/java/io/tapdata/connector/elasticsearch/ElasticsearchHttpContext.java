package io.tapdata.connector.elasticsearch;

import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class ElasticsearchHttpContext {

    private static final String ELASTIC_SEARCH_SCHEME_HTTP = "http";
    private static final String ELASTIC_SEARCH_SCHEME_HTTPS = "https";
    private static final int SOCKET_TIMEOUT_MILLIS = 1000000;
    private static final int CONNECT_TIMEOUT_MILLIS = 5000;
    private static final int CONNECTION_REQUEST_TIMEOUT_MILLIS = 50000;

    private final ElasticsearchConfig elasticsearchConfig;
    private final RestHighLevelClient elasticsearchClient;

    public ElasticsearchHttpContext(ElasticsearchConfig elasticsearchConfig) {
        this.elasticsearchConfig = elasticsearchConfig;
        BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        if (EmptyKit.isNotBlank(elasticsearchConfig.getUser()) && EmptyKit.isNotBlank(elasticsearchConfig.getPassword())) {
            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticsearchConfig.getUser(), elasticsearchConfig.getPassword()));
        }
        HttpHost httpHost;
        if (elasticsearchConfig.isSslValidate()) {
            httpHost = new HttpHost(elasticsearchConfig.getHost(), elasticsearchConfig.getPort(), ELASTIC_SEARCH_SCHEME_HTTPS);
        } else {
            httpHost = new HttpHost(elasticsearchConfig.getHost(), elasticsearchConfig.getPort(), ELASTIC_SEARCH_SCHEME_HTTP);
        }
        elasticsearchClient = new RestHighLevelClient(RestClient.builder(httpHost)
                .setRequestConfigCallback(builder -> builder
                        .setSocketTimeout(SOCKET_TIMEOUT_MILLIS)
                        .setConnectTimeout(CONNECT_TIMEOUT_MILLIS)
                        .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MILLIS))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> getHttpAsyncClientBuilder(httpAsyncClientBuilder, basicCredentialsProvider)));
    }

    private HttpAsyncClientBuilder getHttpAsyncClientBuilder(HttpAsyncClientBuilder httpAsyncClientBuilder, BasicCredentialsProvider basicCredentialsProvider) {
        HttpAsyncClientBuilder clientBuilder = httpAsyncClientBuilder
                .setDefaultCredentialsProvider(basicCredentialsProvider);

        if (elasticsearchConfig.isSslValidate()) {
            SSLContext sslContext = createSSLContext();
            clientBuilder.setSSLContext(sslContext);
            if (!elasticsearchConfig.isValidateCA()) {
                clientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
            }
        }
        return clientBuilder;
    }

    private SSLContext createSSLContext() {
        if (!elasticsearchConfig.isValidateCA()) {
            try {
                SSLContext sslContext = SSLContextBuilder.create()
                        .loadTrustMaterial((chain, authType) -> true) // 允许所有证书 (仅用于测试)
                        .build();
                return sslContext;
            } catch (Exception e) {
                throw new RuntimeException("create ssl context failed");
            }
        }
        String httpCert = elasticsearchConfig.getSslCA();
        if (StringUtils.isBlank(elasticsearchConfig.getSslCA())) {
            throw new RuntimeException("create ssl context failed, ssl certificate can't be empty");
        }
        if (httpCert.contains("-----BEGIN CERTIFICATE-----") && httpCert.contains("-----END CERTIFICATE-----")) {
            return createSSLContextByPem(httpCert);
        } else {
            //p12
            return createSSLContextByP12(httpCert);
        }

    }

    private static SSLContext createSSLContextByPem(String httpCert) {
        try (InputStream certificates = new ByteArrayInputStream(httpCert.getBytes())) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate esCert = (X509Certificate) cf.generateCertificate(certificates);

            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
            keyStore.setCertificateEntry("esCert", esCert);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);

            SSLContext sslContext = SSLContextBuilder.create()
                    .loadTrustMaterial(keyStore, null)
                    .build();
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException("create ssl context failed");
        }
    }
    protected static SSLContext createSSLContextByP12(String httpCert) {
        try (InputStream certificates = new ByteArrayInputStream(httpCert.getBytes())) {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(certificates, null);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException("create ssl context failed");
        }
    }

    public ElasticsearchConfig getElasticsearchConfig() {
        return elasticsearchConfig;
    }

    public RestHighLevelClient getElasticsearchClient() {
        return elasticsearchClient;
    }

    public String queryVersion() {
        try {
            return elasticsearchClient.info(RequestOptions.DEFAULT).getVersion().getNumber();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int countIndices() throws IOException {
        GetAliasesResponse getAliasesResponse = elasticsearchClient.indices().getAlias(new GetAliasesRequest(), RequestOptions.DEFAULT);
        return getAliasesResponse.getAliases().size();
    }

    public boolean existsIndex(String index) throws IOException {
        return elasticsearchClient.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
    }

    public void finish() throws IOException {
        elasticsearchClient.close();
    }

}
