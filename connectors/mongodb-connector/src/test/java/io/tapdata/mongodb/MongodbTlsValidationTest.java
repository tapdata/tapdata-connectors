package io.tapdata.mongodb;

import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.util.SSLUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.io.InputStream;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;

class MongodbTlsValidationTest {
    @ParameterizedTest
    @ValueSource(strings = {"tlsAllowInvalidCertificates", "sslAllowInvalidCertificates", "tlsInsecure",
            "TLSALLOWINVALIDCERTIFICATES"})
    void explicitCertificateBypassIsRejected(String option) {
        MongodbConfig config = new MongodbConfig();
        config.setWriteConcern("ACKNOWLEDGED");
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> MongodbUtil.getMongoClientSettingsBuilder("mongodb://localhost/test?" + option + "=true", config));
        assertTrue(failure.getMessage().contains("trusted CA"));
    }

    @Test
    void ordinaryAndVerifiedTlsConnectionsCanStillBeConfigured() {
        MongodbConfig config = new MongodbConfig();
        config.setWriteConcern("ACKNOWLEDGED");
        assertFalse(MongodbUtil.getMongoClientSettingsBuilder("mongodb://localhost/test", config)
                .build().getSslSettings().isEnabled());
        assertTrue(MongodbUtil.getMongoClientSettingsBuilder("mongodb://localhost/test?tls=true", config)
                .build().getSslSettings().isEnabled());
    }

    @Test
    void missingCustomCaUsesJvmTrustAndRejectsUntrustedCertificate() throws Exception {
        X509Certificate certificate = certificate();
        TrustManager[] managers = SSLUtil.createTrustManagers(Collections.emptyList(), null);
        X509TrustManager trust = (X509TrustManager) managers[0];
        assertTrue(trust.getAcceptedIssuers().length > 0);
        assertThrows(CertificateException.class,
                () -> trust.checkServerTrusted(new X509Certificate[]{certificate}, "RSA"));
    }

    @Test
    void explicitlyTrustedCaCanValidateCertificate() throws Exception {
        X509Certificate certificate = certificate();
        String encoded = java.util.Base64.getEncoder().encodeToString(certificate.getEncoded());
        X509TrustManager trust = (X509TrustManager) SSLUtil.createTrustManagers(Collections.singletonList(encoded), "")[0];
        assertDoesNotThrow(() -> trust.checkServerTrusted(new X509Certificate[]{certificate}, "RSA"));
    }

    @Test
    void caOnlyTlsDoesNotRequireClientIdentity() throws Exception {
        X509Certificate certificate = certificate();
        String encoded = java.util.Base64.getEncoder().encodeToString(certificate.getEncoded());
        javax.net.ssl.SSLContext context = SSLUtil.createSSLContext(null, Collections.emptyList(),
                Collections.singletonList(encoded), null);
        assertEquals("TLSv1.2", context.getProtocol());
        assertNotNull(context.getSocketFactory());

        MongodbConfig config = new MongodbConfig();
        config.setWriteConcern("ACKNOWLEDGED");
        config.setSsl(true);
        config.setSslValidate(true);
        config.setCheckServerIdentity(true);
        config.setSslCA("-----BEGIN CERTIFICATE-----\n" + encoded + "\n-----END CERTIFICATE-----");
        com.mongodb.connection.SslSettings settings = MongodbUtil.getMongoClientSettingsBuilder(
                "mongodb://localhost/test", config).build().getSslSettings();
        assertTrue(settings.isEnabled());
        assertFalse(settings.isInvalidHostNameAllowed());
        assertNotNull(settings.getContext());
    }

    @Test
    void partialClientIdentityIsRejectedInsteadOfSilentlyDisablingAuthentication() throws Exception {
        String encoded = java.util.Base64.getEncoder().encodeToString(certificate().getEncoded());
        assertThrows(IllegalArgumentException.class, () -> SSLUtil.createSSLContext(
                null, Collections.singletonList(encoded), null, null));
        assertThrows(IllegalArgumentException.class, () -> SSLUtil.createSSLContext(
                "a-key-without-certificates", Collections.emptyList(), null, ""));
        assertDoesNotThrow(() -> SSLUtil.createSSLContext("", null, null, ""));
    }

    @Test
    void caBundleTrustsEveryConfiguredCertificate() throws Exception {
        java.security.KeyPairGenerator generator = java.security.KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        java.security.KeyPair pair = generator.generateKeyPair();
        javax.security.auth.x500.X500Principal name = new javax.security.auth.x500.X500Principal("CN=Second Test CA");
        long now = System.currentTimeMillis();
        org.bouncycastle.cert.X509CertificateHolder holder = new org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder(
                name, java.math.BigInteger.ONE, new java.util.Date(now - 60000),
                new java.util.Date(now + 86400000), name, pair.getPublic())
                .build(new org.bouncycastle.operator.jcajce.JcaContentSignerBuilder("SHA256withRSA").build(pair.getPrivate()));
        X509Certificate second = new org.bouncycastle.cert.jcajce.JcaX509CertificateConverter().getCertificate(holder);
        X509Certificate first = certificate();
        java.util.Base64.Encoder encoder = java.util.Base64.getEncoder();
        java.util.List<String> chain = java.util.Arrays.asList(encoder.encodeToString(first.getEncoded()),
                encoder.encodeToString(second.getEncoded()));
        X509TrustManager trust = (X509TrustManager) SSLUtil.createTrustManagers(chain, null)[0];
        assertDoesNotThrow(() -> trust.checkServerTrusted(new X509Certificate[]{first}, "RSA"));
        assertDoesNotThrow(() -> trust.checkServerTrusted(new X509Certificate[]{second}, "RSA"));

        String key = encoder.encodeToString(pair.getPrivate().getEncoded());
        javax.net.ssl.SSLContext context = SSLUtil.createSSLContext(key,
                Collections.singletonList(encoder.encodeToString(second.getEncoded())), chain, "");
        assertEquals("TLSv1.2", context.getProtocol());
    }

    @Test
    void legacySslFlagUsesJvmTrustAndRetainsHostnamePreference() {
        MongodbConfig config = new MongodbConfig();
        config.setWriteConcern("ACKNOWLEDGED");
        config.setSsl(true);
        config.setSslValidate(false);
        config.setCheckServerIdentity(false);
        com.mongodb.connection.SslSettings settings = MongodbUtil.getMongoClientSettingsBuilder(
                "mongodb://localhost/test", config).build().getSslSettings();
        assertTrue(settings.isEnabled());
        assertTrue(settings.isInvalidHostNameAllowed());
        assertNotNull(settings.getContext());
    }

    private X509Certificate certificate() throws Exception {
        try (InputStream input = getClass().getResourceAsStream("/tls/sonar-test-ca.pem")) {
            assertNotNull(input);
            return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
        }
    }
}
