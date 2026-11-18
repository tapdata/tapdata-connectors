package io.tapdata.connector.doris.streamload;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class StreamLoadRedirectTest {
    private static final byte[] DATA = "[{\"id\":1}]".getBytes(StandardCharsets.UTF_8);

    private ReplayableStreamLoadEntity entity(StreamLoadDiagnostics d) throws Exception {
        RecordStream stream = new RecordStream(1024, 3);
        stream.startInput();
        stream.write(DATA);
        stream.endInput();
        return new ReplayableStreamLoadEntity(stream, d);
    }

    @Test
    void replaysIdenticalPutAfterEarlyContinueAnd307ForBothFactories() throws Exception {
        for (boolean tlsFactory : new boolean[]{false, true}) {
            try (ScriptServer server = new ScriptServer(307, true, 0);
                 CloseableHttpClient client = tlsFactory ? HttpUtil.generationHttpClient() : new HttpUtil().getHttpClient()) {
                StreamLoadDiagnostics d = new StreamLoadDiagnostics();
                try (CloseableHttpResponse response = client.execute(request(server, d), context(d))) {
                    assertEquals(200, response.getStatusLine().getStatusCode());
                }
                List<Captured> requests = server.result.get(10, TimeUnit.SECONDS);
                assertEquals(2, requests.size());
                assertTrue(requests.get(0).headers.startsWith("PUT /fe HTTP/1.1"));
                assertTrue(requests.get(1).headers.startsWith("PUT /be HTTP/1.1"));
                for (Captured captured : requests) {
                    assertArrayEquals(DATA, captured.body);
                    assertTrue(captured.headers.contains("label: same-label"));
                    assertTrue(captured.headers.contains("Authorization: Basic test-secret"));
                }
                assertEquals(2, d.bodyWrites);
                assertEquals(DATA.length * 2, d.bytesWritten);
                assertTrue(d.toString().contains("status=307"));
                assertTrue(d.toString().contains("peer=/127.0.0.1:"), d.toString());
                assertFalse(d.toString().contains("test-secret"));
            }
        }
    }

    @Test
    void waitsBeyondOldThreeSecondLimitWithoutSendingBodyToFe() throws Exception {
        try (ScriptServer server = new ScriptServer(307, false, 3200);
             CloseableHttpClient client = new HttpUtil().getHttpClient()) {
            StreamLoadDiagnostics d = new StreamLoadDiagnostics();
            try (CloseableHttpResponse response = client.execute(request(server, d), context(d))) {
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
            List<Captured> requests = server.result.get(10, TimeUnit.SECONDS);
            assertEquals(0, requests.get(0).body.length);
            assertArrayEquals(DATA, requests.get(1).body);
            assertEquals(1, d.bodyWrites);
        }
    }

    @Test
    void supports308ButDoesNotConvert302ToGet() throws Exception {
        for (int code : new int[]{308, 302}) {
            try (ScriptServer server = new ScriptServer(code, false, 0);
                 CloseableHttpClient client = HttpUtil.generationHttpClient()) {
                StreamLoadDiagnostics d = new StreamLoadDiagnostics();
                try (CloseableHttpResponse response = client.execute(request(server, d), context(d))) {
                    assertEquals(code == 308 ? 200 : 302, response.getStatusLine().getStatusCode());
                }
                List<Captured> requests = server.result.get(10, TimeUnit.SECONDS);
                assertEquals(code == 308 ? 2 : 1, requests.size());
            }
        }
    }

    @Test
    void rejectsLoopsAndMissingLocations() throws Exception {
        for (String location : new String[]{"/fe", ""}) {
            try (ScriptServer server = new ScriptServer(307, false, 0, location);
                 CloseableHttpClient client = new HttpUtil().getHttpClient()) {
                StreamLoadDiagnostics d = new StreamLoadDiagnostics();
                IOException failure = assertThrows(IOException.class, () -> client.execute(request(server, d), context(d)));
                String summary = d.failureSummary(failure);
                assertTrue(summary.contains("ClientProtocolException"), summary);
                assertFalse(summary.contains("redirectFailure=none"), summary);
                assertTrue(summary.contains("phase=redirect_validation"), summary);
                assertFalse(summary.contains("password"), summary);
                assertTrue(d.hops.size() <= 6, d.toString());
                assertFalse(d.toString().contains("password"));
            }
        }
    }

    @Test
    void stopsAtMaximumRedirects() throws Exception {
        try (ScriptServer server = new ScriptServer(307, false, 0, "chain");
             CloseableHttpClient client = new HttpUtil().getHttpClient()) {
            StreamLoadDiagnostics d = new StreamLoadDiagnostics();
            IOException failure = assertThrows(IOException.class, () -> client.execute(request(server, d), context(d)));
            assertTrue(d.failureSummary(failure).contains("maximum_redirects_exceeded"), d.failureSummary(failure));
            assertEquals(HttpUtil.MAX_REDIRECTS + 1, server.result.get(10, TimeUnit.SECONDS).size());
        }
    }

    @Test
    void redirectResponseIsNeverLoadSuccess() throws Exception {
        DorisStreamLoader loader = org.mockito.Mockito.mock(DorisStreamLoader.class,
                org.mockito.Mockito.CALLS_REAL_METHODS);
        CloseableHttpResponse response = org.mockito.Mockito.mock(CloseableHttpResponse.class);
        org.mockito.Mockito.when(response.getStatusLine()).thenReturn(
                new org.apache.http.message.BasicStatusLine(org.apache.http.HttpVersion.HTTP_1_1, 307, "Temporary Redirect"));
        assertThrows(io.tapdata.connector.doris.streamload.exception.DorisRetryableException.class,
                () -> loader.handlePreCommitResponse(response));
    }

    @Test
    void snapshotCrossesBuffersAndDrainsEndMarkerForNextBatch() throws Exception {
        RecordStream stream = new RecordStream(1024, 3);
        byte[] data = new byte[1500];
        new Random(1).nextBytes(data);
        for (int i = 0; i < 2; i++) {
            stream.setContentLength(0);
            stream.startInput();
            stream.write(data);
            stream.endInput();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            new ReplayableStreamLoadEntity(stream, new StreamLoadDiagnostics()).writeTo(out);
            assertArrayEquals(data, out.toByteArray());
        }
    }

    @Test
    void redactsUrlSecretsAndInvalidInput() {
        assertEquals("https://example.com:8040", StreamLoadDiagnostics.endpoint("https://user:secret@example.com:8040/api/db/t?token=secret#secret"));
        assertEquals("invalid", StreamLoadDiagnostics.endpoint("http://bad host/secret"));
        assertEquals("missing", StreamLoadDiagnostics.endpoint(null));
        assertEquals("http://be_node:8040", StreamLoadDiagnostics.endpoint("http://user:password@be_node:8040/api?token=secret#secret"));
    }

    @Test
    void snapshotsDrainBatchAndRemainReplayable() throws Exception {
        StreamLoadDiagnostics d = new StreamLoadDiagnostics();
        ReplayableStreamLoadEntity entity = entity(d);
        ByteArrayOutputStream first = new ByteArrayOutputStream();
        ByteArrayOutputStream second = new ByteArrayOutputStream();
        entity.writeTo(first);
        entity.writeTo(second);
        assertTrue(entity.isRepeatable());
        assertEquals(DATA.length, entity.getContentLength());
        assertArrayEquals(DATA, first.toByteArray());
        assertArrayEquals(DATA, second.toByteArray());
    }

    @Test
    void nullProtocolMessageStillReportsNestedReasonWithoutUrlSecrets() {
        StreamLoadDiagnostics d = new StreamLoadDiagnostics();
        Throwable duplicate = new org.apache.http.client.ClientProtocolException(
                new org.apache.http.ProtocolException("Content-Length header already present"));
        assertNull(duplicate.getMessage());
        assertTrue(d.failureSummary(duplicate).contains("ClientProtocolException -> ProtocolException[duplicate_content_length]"));
        Throwable unsafe = new org.apache.http.client.ClientProtocolException(
                new org.apache.http.client.CircularRedirectException("http://user:password@host/api/table?token=secret"));
        String summary = d.failureSummary(unsafe);
        assertTrue(summary.contains("circular_redirect"));
        assertFalse(summary.contains("password"));
        assertFalse(summary.contains("secret"));
        assertFalse(summary.contains("/api/table"));
    }

    @Test
    void failureSummaryBoundsCyclesAndDoesNotLogUnrecognizedMessages() {
        StreamLoadDiagnostics d = new StreamLoadDiagnostics();
        Exception first = new Exception("Authorization: secret");
        Exception second = new Exception("row data");
        first.initCause(second);
        second.initCause(first);
        assertEquals("phase=prepare, redirectFailure=none, causeChain=Exception -> Exception", d.failureSummary(first));
    }

    @Test
    void loaderExceptionIncludesSafeProtocolReasonAndPreservesCause() throws Exception {
        io.tapdata.connector.doris.bean.DorisConfig config = org.mockito.Mockito.mock(io.tapdata.connector.doris.bean.DorisConfig.class);
        org.mockito.Mockito.when(config.getWriteFormatEnum()).thenReturn(io.tapdata.connector.doris.bean.DorisConfig.WriteFormat.json);
        org.mockito.Mockito.when(config.getWriteByteBufferCapacity()).thenReturn(1);
        org.mockito.Mockito.when(config.getDorisHttp()).thenReturn("localhost:8030");
        org.mockito.Mockito.when(config.getDatabase()).thenReturn("db");
        io.tapdata.connector.doris.DorisJdbcContext jdbc = org.mockito.Mockito.mock(io.tapdata.connector.doris.DorisJdbcContext.class);
        org.mockito.Mockito.when(jdbc.getConfig()).thenReturn(config);
        CloseableHttpClient client = org.mockito.Mockito.mock(CloseableHttpClient.class);
        IOException protocol = new org.apache.http.client.ClientProtocolException(
                new org.apache.http.ProtocolException("Content-Length header already present"));
        org.mockito.Mockito.when(client.execute(org.mockito.ArgumentMatchers.any(org.apache.http.client.methods.HttpUriRequest.class),
                org.mockito.ArgumentMatchers.any(org.apache.http.protocol.HttpContext.class))).thenThrow(protocol);
        DorisStreamLoader loader = new DorisStreamLoader(jdbc, client);
        io.tapdata.entity.schema.TapTable table = org.mockito.Mockito.mock(io.tapdata.entity.schema.TapTable.class);
        org.mockito.Mockito.when(table.getId()).thenReturn("t");
        org.mockito.Mockito.when(table.getNameFieldMap()).thenReturn(new java.util.LinkedHashMap<>());
        org.springframework.test.util.ReflectionTestUtils.setField(loader, "tapTable", table);
        org.springframework.test.util.ReflectionTestUtils.setField(loader, "dataColumns",
                new java.util.concurrent.atomic.AtomicReference<>(Collections.emptySet()));
        RecordStream stream = (RecordStream) org.springframework.test.util.ReflectionTestUtils.getField(loader, "recordStream");
        stream.startInput();
        stream.write(DATA);
        stream.endInput();
        io.tapdata.connector.doris.streamload.exception.StreamLoadException failure = assertThrows(
                io.tapdata.connector.doris.streamload.exception.StreamLoadException.class, () -> loader.put(table));
        assertSame(protocol, failure.getCause());
        assertTrue(failure.getMessage().contains("duplicate_content_length"), failure.getMessage());
        assertTrue(failure.getMessage().contains("phase=execute"), failure.getMessage());
        assertTrue(failure.getMessage().contains("label=tapdata_"), failure.getMessage());
    }

    @Test
    void httpsToHttpRejectsBeforeCopyingHeadersEvenWithoutDiagnostics() throws Exception {
        org.apache.http.client.RedirectStrategy strategy = (org.apache.http.client.RedirectStrategy)
                org.springframework.test.util.ReflectionTestUtils.getField(builder(), "redirectStrategy");
        for (int code : new int[]{307, 308}) {
            for (boolean diagnostics : new boolean[]{false, true}) {
                org.apache.http.HttpResponse response = new org.apache.http.message.BasicHttpResponse(
                        org.apache.http.HttpVersion.HTTP_1_1, code, "Redirect");
                response.setHeader("Location", "http://localhost:8040/api/db/t?token=secret");
                StreamLoadDiagnostics d = new StreamLoadDiagnostics();
                HttpClientContext context = diagnostics ? context(d) : HttpClientContext.create();
                context.setTargetHost(new org.apache.http.HttpHost("localhost", 443, "https"));
                HttpPut request = new HttpPut("https://localhost/fe");
                request.setHeader("Authorization", "Basic test-secret");
                org.apache.http.ProtocolException failure = assertThrows(org.apache.http.ProtocolException.class,
                        () -> strategy.getRedirect(request, response, context));
                assertEquals("Refusing HTTPS to HTTP Stream Load redirect", failure.getMessage());
                if (diagnostics) {
                    assertEquals("https_to_http_redirect_rejected", d.redirectFailure);
                    assertTrue(d.failureSummary(failure).contains("ProtocolException[https_to_http_redirect_rejected]"));
                    assertTrue(d.toString().contains("httpsToHttp=true"), d.toString());
                    assertFalse(d.toString().contains("secret"));
                }
            }
        }
    }

    @Test
    void realTlsRedirectNeverConnectsToHttpTargetAndStillAllowsHttps() throws Exception {
        // Public test fixture only: its key/password must never be used outside these loopback tests.
        java.security.KeyStore keys = java.security.KeyStore.getInstance("PKCS12");
        try (InputStream fixture = getClass().getResourceAsStream("/streamload/localhost.p12")) {
            keys.load(fixture, "test-only".toCharArray());
        }
        javax.net.ssl.SSLContext tls = org.apache.http.ssl.SSLContexts.custom()
                .loadKeyMaterial(keys, "test-only".toCharArray()).loadTrustMaterial(keys, null).build();
        for (boolean tlsFactory : new boolean[]{false, true}) {
            for (int code : new int[]{307, 308}) {
                for (boolean downgrade : new boolean[]{true, false}) {
                    try (ServerSocket httpTarget = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"));
                         CloseableHttpClient client = tlsFactory ? HttpUtil.generationHttpClient()
                                 : builder().setSSLContext(tls).build()) {
                        com.sun.net.httpserver.HttpsServer fe = com.sun.net.httpserver.HttpsServer.create(
                                new InetSocketAddress("127.0.0.1", 0), 0);
                        fe.setHttpsConfigurator(new com.sun.net.httpserver.HttpsConfigurator(tls));
                        java.util.concurrent.atomic.AtomicInteger beRequests = new java.util.concurrent.atomic.AtomicInteger();
                        java.util.concurrent.atomic.AtomicReference<String> authorization = new java.util.concurrent.atomic.AtomicReference<>();
                        java.util.concurrent.atomic.AtomicReference<byte[]> body = new java.util.concurrent.atomic.AtomicReference<>();
                        fe.createContext("/fe", exchange -> {
                            exchange.getRequestBody().close();
                            exchange.getResponseHeaders().set("Location", downgrade
                                    ? "http://localhost:" + httpTarget.getLocalPort() + "/be?token=secret"
                                    : "https://localhost:" + fe.getAddress().getPort() + "/be");
                            exchange.sendResponseHeaders(code, -1);
                            exchange.close();
                        });
                        fe.createContext("/be", exchange -> {
                            beRequests.incrementAndGet();
                            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                            ByteArrayOutputStream received = new ByteArrayOutputStream();
                            byte[] buffer = new byte[1024];
                            int count;
                            while ((count = exchange.getRequestBody().read(buffer)) != -1) received.write(buffer, 0, count);
                            body.set(received.toByteArray());
                            exchange.sendResponseHeaders(200, -1);
                            exchange.close();
                        });
                        fe.start();
                        try {
                            StreamLoadDiagnostics d = new StreamLoadDiagnostics();
                            HttpPut request = new HttpPut("https://localhost:" + fe.getAddress().getPort() + "/fe");
                            request.setHeader("Authorization", "Basic test-secret");
                            request.setHeader("Expect", "100-continue");
                            request.setEntity(entity(d));
                            if (downgrade) {
                                org.apache.http.client.ClientProtocolException failure = assertThrows(
                                        org.apache.http.client.ClientProtocolException.class,
                                        () -> client.execute(request, context(d)));
                                assertEquals("https_to_http_redirect_rejected", d.redirectFailure);
                                assertTrue(d.failureSummary(failure).contains("https_to_http_redirect_rejected"));
                                assertFalse(d.toString().contains("secret"));
                                assertEquals(0, beRequests.get());
                            } else {
                                try (CloseableHttpResponse response = client.execute(request, context(d))) {
                                    assertEquals(200, response.getStatusLine().getStatusCode());
                                }
                                assertEquals(1, beRequests.get());
                                assertEquals("Basic test-secret", authorization.get());
                                assertArrayEquals(DATA, body.get());
                            }
                            // A queued connection would prove credentials/data could reach plaintext HTTP.
                            httpTarget.setSoTimeout(100);
                            assertThrows(SocketTimeoutException.class, () -> {
                                try (Socket unexpected = httpTarget.accept()) {
                                    fail("HTTP target received a connection");
                                }
                            });
                        } finally {
                            fe.stop(0);
                        }
                    }
                }
            }
        }
    }

    @Test
    void compatibleLocationsCompletePutWithIdenticalBodyAndLabel() throws Exception {
        for (boolean tlsFactory : new boolean[]{false, true}) {
            for (String location : new String[]{
                    "http://localhost:PORT/be#fragment-secret",
                    "http://user:password@localhost:PORT/be?token=query-secret"}) {
                try (ScriptServer server = new ScriptServer(307, true, 0, "compat:" + location);
                     CloseableHttpClient client = tlsFactory ? HttpUtil.generationHttpClient() : new HttpUtil().getHttpClient()) {
                    StreamLoadDiagnostics d = new StreamLoadDiagnostics();
                    try (CloseableHttpResponse response = client.execute(request(server, d), context(d))) {
                        assertEquals(200, response.getStatusLine().getStatusCode());
                    }
                    List<Captured> requests = server.result.get(10, TimeUnit.SECONDS);
                    assertEquals(2, requests.size());
                    for (Captured captured : requests) {
                        assertArrayEquals(DATA, captured.body);
                        assertTrue(captured.headers.startsWith("PUT "));
                        assertTrue(captured.headers.contains("label: same-label"));
                    }
                    assertFalse(d.toString().contains("password"));
                    assertFalse(d.toString().contains("fragment-secret"));
                    assertFalse(d.toString().contains("query-secret"));
                }
            }
        }
    }

    @Test
    void underscoreAuthorityRoutesEvenWhenJavaUriHostIsNull() throws Exception {
        assertNull(URI.create("http://be_node:8040/be").getHost());
        for (int code : new int[]{307, 308}) {
            try (ScriptServer server = new ScriptServer(code, true, 0, "compat:http://be_node:PORT/be");
                 CloseableHttpClient client = builder().setDnsResolver(host -> {
                     assertTrue("be_node".equals(host) || "127.0.0.1".equals(host), host);
                     return new InetAddress[]{InetAddress.getByName("127.0.0.1")};
                 }).build()) {
                StreamLoadDiagnostics d = new StreamLoadDiagnostics();
                try (CloseableHttpResponse response = client.execute(request(server, d), context(d))) {
                    assertEquals(200, response.getStatusLine().getStatusCode());
                }
                List<Captured> requests = server.result.get(10, TimeUnit.SECONDS);
                assertEquals(2, requests.size());
                assertTrue(requests.get(1).headers.startsWith("PUT /be HTTP/1.1"));
                assertTrue(requests.get(1).headers.contains("label: same-label"));
                assertArrayEquals(DATA, requests.get(1).body);
                assertTrue(d.toString().contains("javaHostMissing=true"), d.toString());
                assertTrue(d.toString().contains("http://be_node:"), d.toString());
            }
        }
    }

    private org.apache.http.impl.client.HttpClientBuilder builder() throws Exception {
        java.lang.reflect.Method factory = HttpUtil.class.getDeclaredMethod("builder");
        factory.setAccessible(true);
        return (org.apache.http.impl.client.HttpClientBuilder) factory.invoke(null);
    }

    private HttpClientContext context(StreamLoadDiagnostics d) {
        HttpClientContext context = HttpClientContext.create();
        context.setAttribute(StreamLoadDiagnostics.KEY, d);
        return context;
    }

    private HttpPut request(ScriptServer server, StreamLoadDiagnostics d) throws Exception {
        HttpPut request = new HttpPut("http://127.0.0.1:" + server.socket.getLocalPort() + "/fe");
        request.setHeader("Expect", "100-continue");
        request.setHeader("label", "same-label");
        request.setHeader("Authorization", "Basic test-secret");
        request.setEntity(entity(d));
        return request;
    }

    private static class Captured {
        final String headers;
        final byte[] body;
        Captured(String headers, byte[] body) { this.headers = headers; this.body = body; }
    }

    /** Raw HTTP peer: unlike HttpServer, it can deliberately withhold 100 Continue. */
    private static class ScriptServer implements AutoCloseable {
        final ServerSocket socket = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"));
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<List<Captured>> result;

        ScriptServer(int code, boolean sendContinue, long delay) throws IOException {
            this(code, sendContinue, delay, null);
        }

        ScriptServer(int code, boolean sendContinue, long delay, String overrideLocation) throws IOException {
            socket.setSoTimeout(5000);
            result = executor.submit(() -> {
                boolean compatible = overrideLocation != null && overrideLocation.startsWith("compat:");
                List<Captured> captured = new ArrayList<>();
                int count = "chain".equals(overrideLocation) ? HttpUtil.MAX_REDIRECTS + 1 : (code == 307 || code == 308) && (overrideLocation == null || compatible || "/fe".equals(overrideLocation)) ? 2 : 1;
                for (int i = 0; i < count; i++) {
                    try (Socket connection = socket.accept()) {
                        connection.setSoTimeout(5000);
                        InputStream in = connection.getInputStream();
                        OutputStream out = connection.getOutputStream();
                        StringBuilder headers = new StringBuilder();
                        int state = 0;
                        while (state < 4) {
                            int b = in.read();
                            if (b == -1) throw new EOFException();
                            headers.append((char) b);
                            state = b == (state % 2 == 0 ? '\r' : '\n') ? state + 1 : 0;
                        }
                        byte[] body = new byte[0];
                        if (sendContinue || i > 0) {
                            out.write("HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                            int length = Integer.parseInt(headers.toString().split("(?i)Content-Length: ")[1].split("\r\n")[0]);
                            body = new byte[length];
                            new DataInputStream(in).readFully(body);
                        }
                        captured.add(new Captured(headers.toString(), body));
                        if (delay > 0 && i == 0) Thread.sleep(delay);
                        String location = "chain".equals(overrideLocation) ? "/hop" + i : overrideLocation == null ? "http://localhost:" + socket.getLocalPort() + "/be" : overrideLocation;
                        if (compatible) location = overrideLocation.substring("compat:".length()).replace("PORT", String.valueOf(socket.getLocalPort()));
                        String response = i > 0 && (overrideLocation == null || compatible) ? "200 OK" : code + " Redirect";
                        out.write(("HTTP/1.1 " + response + "\r\nConnection: close\r\nContent-Length: 0\r\n"
                                + (location.isEmpty() ? "" : "Location: " + location + "\r\n") + "\r\n").getBytes(StandardCharsets.US_ASCII));
                        out.flush();
                    }
                }
                return captured;
            });
        }

        @Override
        public void close() throws IOException {
            socket.close();
            executor.shutdownNow();
        }
    }
}
