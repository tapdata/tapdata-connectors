package io.tapdata.connector.paimon.write;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.VersionInfoUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 固定 AWS SDK 1.12.367 的真实 HTTP 请求特征与 ownership 条件写协议测试。
 * localhost 服务的同步临界区只是条件写模型，不构成真实 S3/MinIO 原子性或兼容性证明。
 * 官方契约：If-None-Match: * 防止覆盖；If-Match 比较 ETag；不满足返回 412，且请求须使用 SigV4。
 * https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
 */
class PaimonS3ConditionalWriteContractTest {

    private static final String BUCKET = "test-ownership";
    private static final String KEY = "owner.json";
    private static final String ACCESS_KEY = "dummy-access-key";

    @Test
    void concurrentInitialCreationMustSendSignedIfNoneMatchAndHaveExactlyOneWinner()
            throws Exception {
        assertEquals("1.12.367", VersionInfoUtils.getVersion(), "升级 SDK 后必须重新审查并执行本契约测试");
        try (ConditionalStore store = new ConditionalStore(true)) {
            ExecutorService callers = Executors.newFixedThreadPool(2);
            try {
                String heldA = record("HELD", "A", UUID.randomUUID().toString());
                String heldB = record("HELD", "B", UUID.randomUUID().toString());
                Future<Attempt> a = callers.submit(() -> createAttempt(store, heldA));
                Future<Attempt> b = callers.submit(() -> createAttempt(store, heldB));
                List<Attempt> attempts = Arrays.asList(
                        a.get(15L, TimeUnit.SECONDS), b.get(15L, TimeUnit.SECONDS));

                assertEquals(0L, store.concurrentCreates.getCount(), "两个请求必须均进入真实 HTTP handler 后才开始竞争");
                assertEquals(1L, attempts.stream().filter(attempt -> attempt.failure == null).count());
                Attempt winner = attempts.stream().filter(attempt -> attempt.failure == null).findFirst().get();
                Attempt loser = attempts.stream().filter(attempt -> attempt.failure != null).findFirst().get();
                assertPreconditionFailed(loser.failure);
                assertEquals(winner.body, store.body());
                assertEquals(quoted(winner.result), store.etag());
                assertEquals(2, store.requests.size(), "412 不得被 SDK 自动重试为覆盖写");
                for (RequestCapture request : store.requests) {
                    assertEquals("*", request.ifNoneMatch);
                    assertNull(request.ifMatch);
                    assertSignedV4(request, "if-none-match");
                }
            } finally {
                callers.shutdownNow();
                assertTrue(callers.awaitTermination(10L, TimeUnit.SECONDS), "并发 SDK 调用线程必须实际退出");
            }
        }
    }

    @Test
    void lateReleaseFromOldOwnerMustFailWithOldEtagAndKeepNewOwnerRecord()
            throws Exception {
        try (ConditionalStore store = new ConditionalStore(false)) {
            String heldA = record("HELD", "A", UUID.randomUUID().toString());
            String etagA = quoted(store.put(heldA, "If-None-Match", "*"));
            String free = record("FREE", "", UUID.randomUUID().toString());
            String freeEtag = quoted(store.put(free, "If-Match", etagA));
            String heldB = record("HELD", "B", UUID.randomUUID().toString());
            String etagB = quoted(store.put(heldB, "If-Match", freeEtag));

            AmazonS3Exception lateRelease = assertThrows(AmazonS3Exception.class,
                    () -> store.put(record("FREE", "", UUID.randomUUID().toString()), "If-Match", etagA));

            assertPreconditionFailed(lateRelease);
            assertNotEquals(etagA, etagB);
            assertEquals(heldB, store.body(), "A 的迟到 release 不得覆盖 B 已持有的记录");
            assertEquals(etagB, store.etag());
            assertEquals(4, store.requests.size());
            assertEquals(etagA, store.requests.get(1).ifMatch);
            assertEquals(freeEtag, store.requests.get(2).ifMatch);
            assertEquals(etagA, store.requests.get(3).ifMatch, "SDK 必须保留调用者传入的双引号 ETag");
            for (RequestCapture request : store.requests.subList(1, 4)) {
                assertNull(request.ifNoneMatch);
                assertTrue(request.ifMatch.startsWith("\"") && request.ifMatch.endsWith("\""));
                assertSignedV4(request, "if-match");
            }
        }
    }

    @Test
    void freshNonceOnEveryFreeRecordMustPreventStaleFreeEtagFromPassingAfterAba()
            throws Exception {
        try (ConditionalStore store = new ConditionalStore(false)) {
            String firstNonce = UUID.randomUUID().toString();
            String secondNonce = UUID.randomUUID().toString();
            assertNotEquals(firstNonce, secondNonce);
            String free1 = record("FREE", "", firstNonce);
            String free1Etag = quoted(store.put(free1, "If-None-Match", "*"));
            String heldA = record("HELD", "A", UUID.randomUUID().toString());
            String heldAEtag = quoted(store.put(heldA, "If-Match", free1Etag));
            String free2 = record("FREE", "", secondNonce);
            String free2Etag = quoted(store.put(free2, "If-Match", heldAEtag));
            assertNotEquals(free1Etag, free2Etag, "再次变为 FREE 时内容必须变化，不能复用上一代 FREE 的 ETag");

            String heldB = record("HELD", "B", UUID.randomUUID().toString());
            AmazonS3Exception staleAcquire = assertThrows(AmazonS3Exception.class,
                    () -> store.put(heldB, "If-Match", free1Etag));

            assertPreconditionFailed(staleAcquire);
            assertEquals(free2, store.body());
            assertEquals(free2Etag, store.etag());
            String heldBEtag = quoted(store.put(heldB, "If-Match", free2Etag));
            assertEquals(heldB, store.body(), "使用当前 FREE ETag 的 acquire 才能成功");
            assertEquals(heldBEtag, store.etag());
            assertEquals(5, store.requests.size());
            assertEquals(free1Etag, store.requests.get(3).ifMatch);
            assertEquals(free2Etag, store.requests.get(4).ifMatch);
            for (RequestCapture request : store.requests) {
                assertSignedV4(request, request.ifMatch == null ? "if-none-match" : "if-match");
            }
        }
    }

    private static Attempt createAttempt(ConditionalStore store, String body) {
        try {
            return new Attempt(body, store.put(body, "If-None-Match", "*"), null);
        } catch (AmazonS3Exception failure) {
            return new Attempt(body, null, failure);
        }
    }

    private static void assertPreconditionFailed(AmazonS3Exception failure) {
        assertNotNull(failure);
        assertEquals(412, failure.getStatusCode());
        assertEquals("PreconditionFailed", failure.getErrorCode());
        assertEquals("localhost-request", failure.getRequestId());
    }

    private static void assertSignedV4(RequestCapture request, String conditionHeader) {
        assertEquals("PUT", request.method);
        assertEquals("/" + BUCKET + "/" + KEY, request.path, "endpoint 必须实际使用 path-style 请求");
        assertNotNull(request.authorization);
        assertTrue(request.authorization.startsWith("AWS4-HMAC-SHA256 "));
        assertTrue(request.authorization.contains("Credential=" + ACCESS_KEY + "/"));
        assertTrue(request.authorization.contains("/us-east-1/s3/aws4_request"));
        assertTrue(request.authorization.matches(".*Signature=[0-9a-f]{64}$"));
        assertNotNull(request.amzDate);
        assertTrue(request.amzDate.matches("[0-9]{8}T[0-9]{6}Z"));
        assertNotNull(request.payloadHash);
        int signedHeaders = request.authorization.indexOf("SignedHeaders=");
        assertTrue(signedHeaders >= 0);
        String signed = request.authorization.substring(signedHeaders + "SignedHeaders=".length()).split(",", 2)[0];
        assertTrue(Arrays.asList(signed.split(";")).contains(conditionHeader),
                "条件头必须实际参与 SigV4 签名，而非签名完成后才追加");
    }

    private static String record(String state, String owner, String nonce) {
        return "{\"state\":\"" + state + "\",\"owner\":\"" + owner + "\",\"nonce\":\"" + nonce + "\"}";
    }

    private static String quoted(PutObjectResult result) {
        assertNotNull(result.getETag());
        return "\"" + result.getETag() + "\"";
    }

    private static final class Attempt {
        private final String body;
        private final PutObjectResult result;
        private final AmazonS3Exception failure;

        private Attempt(String body, PutObjectResult result, AmazonS3Exception failure) {
            this.body = body;
            this.result = result;
            this.failure = failure;
        }
    }

    private static final class RequestCapture {
        private final String method;
        private final String path;
        private final String ifNoneMatch;
        private final String ifMatch;
        private final String authorization;
        private final String amzDate;
        private final String payloadHash;

        private RequestCapture(HttpExchange exchange) {
            method = exchange.getRequestMethod();
            path = exchange.getRequestURI().getPath();
            ifNoneMatch = exchange.getRequestHeaders().getFirst("If-None-Match");
            ifMatch = exchange.getRequestHeaders().getFirst("If-Match");
            authorization = exchange.getRequestHeaders().getFirst("Authorization");
            amzDate = exchange.getRequestHeaders().getFirst("X-Amz-Date");
            payloadHash = exchange.getRequestHeaders().getFirst("X-Amz-Content-Sha256");
        }
    }

    private static final class ConditionalStore implements AutoCloseable {
        private final HttpServer server;
        private final ExecutorService handlers = Executors.newFixedThreadPool(4);
        private final AmazonS3 client;
        private final Object stateLock = new Object();
        private final List<RequestCapture> requests = new CopyOnWriteArrayList<>();
        private final CountDownLatch concurrentCreates;
        private String body;
        private String etag;

        private ConditionalStore(boolean synchronizeInitialCreates) throws Exception {
            concurrentCreates = new CountDownLatch(synchronizeInitialCreates ? 2 : 0);
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(handlers);
            server.createContext("/", this::handle);
            server.start();
            try {
                // 版本由模块 pom 与本机 bundle 的 com/amazonaws/sdk/versionInfo.properties 双重核对。
                // SDK 1.12.367 通过 PutObjectRequest.putCustomRequestHeader 传递新条件写请求头。
                client = AmazonS3ClientBuilder.standard()
                        .withEndpointConfiguration(new EndpointConfiguration(
                                "http://127.0.0.1:" + server.getAddress().getPort(), "us-east-1"))
                        .withPathStyleAccessEnabled(true)
                        .withChunkedEncodingDisabled(true)
                        .withCredentials(new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(ACCESS_KEY, "dummy-secret-key")))
                        .withClientConfiguration(new ClientConfiguration()
                                .withMaxErrorRetry(0).withConnectionTimeout(5_000).withSocketTimeout(10_000))
                        .build();
            } catch (RuntimeException | Error failure) {
                server.stop(0);
                handlers.shutdownNow();
                try {
                    if (!handlers.awaitTermination(10L, TimeUnit.SECONDS)) {
                        failure.addSuppressed(new IllegalStateException("HTTP handler 线程未退出"));
                    }
                } catch (InterruptedException interrupted) {
                    failure.addSuppressed(interrupted);
                    Thread.currentThread().interrupt();
                }
                throw failure;
            }
        }

        private PutObjectResult put(String record, String condition, String expected) {
            byte[] bytes = record.getBytes(StandardCharsets.UTF_8);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);
            metadata.setContentType("application/json");
            PutObjectRequest request = new PutObjectRequest(
                    BUCKET, KEY, new ByteArrayInputStream(bytes), metadata);
            request.putCustomRequestHeader(condition, expected);
            return client.putObject(request);
        }

        private String body() {
            synchronized (stateLock) { return body; }
        }

        private String etag() {
            synchronized (stateLock) { return etag; }
        }

        private void handle(HttpExchange exchange) throws IOException {
            try {
                RequestCapture request = new RequestCapture(exchange);
                requests.add(request);
                byte[] bytes = exchange.getRequestBody().readAllBytes();
                if (!"PUT".equals(request.method) || !("/" + BUCKET + "/" + KEY).equals(request.path)) {
                    sendError(exchange, 400, "InvalidRequest");
                    return;
                }
                if ("*".equals(request.ifNoneMatch) && concurrentCreates.getCount() > 0) {
                    concurrentCreates.countDown();
                    try {
                        if (!concurrentCreates.await(5L, TimeUnit.SECONDS)) {
                            sendError(exchange, 500, "FixtureConcurrentRequestTimeout");
                            return;
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        sendError(exchange, 500, "FixtureInterrupted");
                        return;
                    }
                }
                String committedEtag;
                synchronized (stateLock) {
                    boolean create = "*".equals(request.ifNoneMatch) && request.ifMatch == null && body == null;
                    boolean replace = request.ifNoneMatch == null && request.ifMatch != null
                            && request.ifMatch.equals(etag);
                    if (!create && !replace) {
                        sendError(exchange, 412, "PreconditionFailed");
                        return;
                    }
                    body = new String(bytes, StandardCharsets.UTF_8);
                    etag = "\"" + md5(bytes) + "\"";
                    committedEtag = etag;
                }
                exchange.getResponseHeaders().set("ETag", committedEtag);
                exchange.getResponseHeaders().set("x-amz-request-id", "localhost-request");
                exchange.sendResponseHeaders(200, -1);
            } finally {
                exchange.close();
            }
        }

        private static String md5(byte[] bytes) {
            try {
                byte[] digest = MessageDigest.getInstance("MD5").digest(bytes);
                StringBuilder hex = new StringBuilder();
                for (byte value : digest) {
                    hex.append(String.format("%02x", value & 0xff));
                }
                return hex.toString();
            } catch (NoSuchAlgorithmException impossible) {
                throw new AssertionError("JDK 必须提供 MD5", impossible);
            }
        }

        private static void sendError(HttpExchange exchange, int status, String code) throws IOException {
            byte[] xml = ("<Error><Code>" + code + "</Code><Message>local conditional write rejected</Message>"
                    + "<RequestId>localhost-request</RequestId><HostId>localhost-host</HostId></Error>")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/xml");
            exchange.getResponseHeaders().set("x-amz-request-id", "localhost-request");
            exchange.sendResponseHeaders(status, xml.length);
            exchange.getResponseBody().write(xml);
        }

        @Override
        public void close() throws Exception {
            try {
                client.shutdown();
            } finally {
                server.stop(0);
                handlers.shutdownNow();
                assertTrue(handlers.awaitTermination(10L, TimeUnit.SECONDS), "localhost HTTP handler 必须实际退出");
            }
        }
    }
}
