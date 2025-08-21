package io.tapdata.kit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

public final class HttpKit {
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private HttpKit() {
    }

    // GET请求（带查询参数）
    public static String get(String url, Map<String, String> params) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(buildUriWithParams(url, params))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return sendRequest(request);
    }

    // POST表单请求（x-www-form-urlencoded）
    public static String postForm(String url, Map<String, String> formData) {
        String body = buildFormData(formData);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return sendRequest(request);
    }

    // POST JSON请求
    public static String postJson(String url, String json) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return sendRequest(request);
    }

    // 通用请求发送方法
    private static String sendRequest(HttpRequest request) {
        try {
            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return response.body();
            } else {
                throw new RuntimeException("HTTP Error: " + response.statusCode() + " - " + response.body());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("HTTP Request Failed: " + e.getMessage(), e);
        }
    }

    // 构建带查询参数的URI
    private static URI buildUriWithParams(String url, Map<String, String> params) {
        if (params == null || params.isEmpty()) return URI.create(url);

        StringBuilder query = new StringBuilder();
        params.forEach((key, value) -> {
            if (!query.isEmpty()) query.append('&');
            query.append(encode(key)).append('=').append(encode(value));
        });

        String baseUrl = url.contains("?") ? url + "&" : url + "?";
        return URI.create(baseUrl + query);
    }

    // 构建表单请求体
    private static String buildFormData(Map<String, String> formData) {
        if (formData == null || formData.isEmpty()) return "";

        StringBuilder body = new StringBuilder();
        formData.forEach((key, value) -> {
            if (!body.isEmpty()) body.append('&');
            body.append(encode(key)).append('=').append(encode(value));
        });
        return body.toString();
    }

    // URL编码工具方法
    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    public static String sendHttp09Request(String host, int port, String data) throws IOException {
        return sendHttp09Request(host, port, data, 5000); // 默认5秒超时
    }

    public static String sendHttp09Request(String host, int port, String data, int timeoutMs) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeoutMs); // 使用参数化的超时时间

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // 构建HTTP/0.9请求
            String request = "POST /\r\n" +
                    "Content-Type: application/json\r\n" +
                    "Content-Length: " + data.length() + "\r\n" +
                    "\r\n" +
                    data;

            // 发送请求
            out.write(request.getBytes(StandardCharsets.UTF_8));
            out.flush();

            // 优化的响应读取 - 使用智能检测而非固定超时
            return readResponseSmart(in, timeoutMs);
        }
    }

    /**
     * 智能读取响应，避免不必要的等待
     */
    private static String readResponseSmart(InputStream in, int timeoutMs) throws IOException {
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192]; // 增大缓冲区

        long startTime = System.currentTimeMillis();
        long lastDataTime = startTime;
        boolean hasReceivedData = false;

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            try {
                // 检查是否有数据可读
                if (in.available() > 0) {
                    int bytesRead = in.read(buffer);
                    if (bytesRead > 0) {
                        responseBuffer.write(buffer, 0, bytesRead);
                        lastDataTime = System.currentTimeMillis();
                        hasReceivedData = true;
                    } else if (bytesRead == -1) {
                        // 流结束
                        break;
                    }
                } else {
                    // 如果已经接收到数据，并且超过200ms没有新数据，认为接收完成
                    if (hasReceivedData && (System.currentTimeMillis() - lastDataTime > 200)) {
                        break;
                    }

                    // 短暂休眠，避免CPU占用过高
                    try {
                        Thread.sleep(10); // 减少到10ms，提高响应性
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } catch (java.net.SocketTimeoutException e) {
                // Socket超时，如果已经有数据则返回，否则抛出异常
                if (hasReceivedData) {
                    break;
                } else {
                    throw e;
                }
            }
        }

        return responseBuffer.toString(StandardCharsets.UTF_8);
    }

    /**
     * 高性能版本 - 支持自定义超时和连接复用
     */
    public static String sendHttp09RequestFast(String host, int port, String data, int timeoutMs) throws IOException {
        try (Socket socket = new Socket()) {
            // 设置连接超时
            socket.connect(new java.net.InetSocketAddress(host, port), Math.min(timeoutMs, 3000));
            socket.setSoTimeout(timeoutMs);

            // 优化Socket参数
            socket.setTcpNoDelay(true); // 禁用Nagle算法，减少延迟
            socket.setKeepAlive(false); // 短连接，不需要keep-alive

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // 构建HTTP/0.9请求
            String request = "POST /\r\n" +
                    "Content-Type: application/json\r\n" +
                    "Content-Length: " + data.length() + "\r\n" +
                    "\r\n" +
                    data;

            // 发送请求
            out.write(request.getBytes(StandardCharsets.UTF_8));
            out.flush();

            // 高性能响应读取
            return readResponseFast(in, timeoutMs);
        }
    }

    /**
     * 高性能响应读取
     */
    private static String readResponseFast(InputStream in, int timeoutMs) throws IOException {
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        byte[] buffer = new byte[16384]; // 更大的缓冲区

        long startTime = System.currentTimeMillis();
        long lastDataTime = startTime;
        boolean hasReceivedData = false;
        int consecutiveEmptyReads = 0;

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            try {
                int available = in.available();
                if (available > 0) {
                    int bytesRead = in.read(buffer, 0, Math.min(buffer.length, available));
                    if (bytesRead > 0) {
                        responseBuffer.write(buffer, 0, bytesRead);
                        lastDataTime = System.currentTimeMillis();
                        hasReceivedData = true;
                        consecutiveEmptyReads = 0;
                    } else if (bytesRead == -1) {
                        break; // 流结束
                    }
                } else {
                    consecutiveEmptyReads++;

                    // 智能退出条件
                    if (hasReceivedData) {
                        long timeSinceLastData = System.currentTimeMillis() - lastDataTime;
                        // 如果已经接收到数据，并且100ms内没有新数据，认为接收完成
                        if (timeSinceLastData > 100) {
                            break;
                        }
                        // 或者连续多次读取为空
                        if (consecutiveEmptyReads > 10) {
                            break;
                        }
                    }

                    // 动态休眠时间
                    int sleepTime = hasReceivedData ? 5 : 20; // 有数据后更频繁检查
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } catch (java.net.SocketTimeoutException e) {
                if (hasReceivedData) {
                    break; // 已有数据，正常退出
                } else {
                    throw new IOException("Request timeout after " + timeoutMs + "ms", e);
                }
            }
        }

        if (!hasReceivedData) {
            throw new IOException("No response received within " + timeoutMs + "ms");
        }

        return responseBuffer.toString(StandardCharsets.UTF_8);
    }

    /**
     * 批量发送请求 - 进一步提高QPS
     */
    public static java.util.List<String> sendHttp09RequestBatch(String host, int port,
            java.util.List<String> dataList, int timeoutMs) throws IOException {

        java.util.List<String> responses = new java.util.ArrayList<>();

        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), Math.min(timeoutMs, 3000));
            socket.setSoTimeout(timeoutMs);
            socket.setTcpNoDelay(true);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            for (String data : dataList) {
                // 发送请求
                String request = "POST /\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length: " + data.length() + "\r\n" +
                        "\r\n" +
                        data;

                out.write(request.getBytes(StandardCharsets.UTF_8));
                out.flush();

                // 读取响应
                String response = readResponseFast(in, timeoutMs);
                responses.add(response);
            }
        }

        return responses;
    }
}
