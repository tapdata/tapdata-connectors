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
    void rejectsLoopsMissingAndUnsafeLocations() throws Exception {
        for (String location : new String[]{"/fe", "", "ftp://localhost/file", "http://user:password@localhost/be"}) {
            try (ScriptServer server = new ScriptServer(307, false, 0, location);
                 CloseableHttpClient client = new HttpUtil().getHttpClient()) {
                StreamLoadDiagnostics d = new StreamLoadDiagnostics();
                assertThrows(IOException.class, () -> client.execute(request(server, d), context(d)));
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
            assertThrows(IOException.class, () -> client.execute(request(server, d), context(d)));
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
                List<Captured> captured = new ArrayList<>();
                int count = "chain".equals(overrideLocation) ? HttpUtil.MAX_REDIRECTS + 1 : (code == 307 || code == 308) && (overrideLocation == null || "/fe".equals(overrideLocation)) ? 2 : 1;
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
                        String response = i > 0 && overrideLocation == null ? "200 OK" : code + " Redirect";
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
