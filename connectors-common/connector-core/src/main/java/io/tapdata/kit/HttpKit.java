package io.tapdata.kit;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class HttpKit {
    private static final int CONNECT_TIMEOUT = 10000; // 10秒
    private static final int READ_TIMEOUT = 10000; // 10秒

    private HttpKit() {
    }

    // GET请求（带查询参数）
    public static String get(String url, Map<String, String> params) throws IOException {
        String fullUrl = buildUrlWithParams(url, params);
        return sendHttpRequest(fullUrl, "GET", null, null);
    }

    // POST表单请求（x-www-form-urlencoded）
    public static String postForm(String url, Map<String, String> formData) throws IOException {
        String body = buildFormData(formData);
        return sendHttpRequest(url, "POST", "application/x-www-form-urlencoded", body);
    }

    // POST JSON请求
    public static String postJson(String url, String json) throws IOException {
        return sendHttpRequest(url, "POST", "application/json", json);
    }

    // 通用HTTP请求发送方法
    private static String sendHttpRequest(String urlString, String method, String contentType, String body) throws IOException {
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        try {
            // 设置请求方法和超时
            connection.setRequestMethod(method);
            connection.setConnectTimeout(CONNECT_TIMEOUT);
            connection.setReadTimeout(READ_TIMEOUT);
            connection.setInstanceFollowRedirects(true);

            // 设置请求头
            if (contentType != null) {
                connection.setRequestProperty("Content-Type", contentType);
            }
            connection.setRequestProperty("User-Agent", "HttpKit/1.0");

            // 如果有请求体，发送数据
            if (body != null && !body.isEmpty()) {
                connection.setDoOutput(true);
                try (OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8.name())) {
                    writer.write(body);
                    writer.flush();
                }
            }

            // 获取响应
            int responseCode = connection.getResponseCode();
            InputStream inputStream;

            if (responseCode >= 200 && responseCode < 300) {
                inputStream = connection.getInputStream();
            } else {
                inputStream = connection.getErrorStream();
            }

            String response = readInputStream(inputStream);

            if (responseCode >= 200 && responseCode < 300) {
                return response;
            } else {
                throw new RuntimeException("HTTP Error: " + responseCode + " - " + response);
            }

        } finally {
            connection.disconnect();
        }
    }

    // 读取输入流内容
    private static String readInputStream(InputStream inputStream) throws IOException {
        if (inputStream == null) {
            return "";
        }

        StringBuilder response = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8.name()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line).append('\n');
            }
        }

        // 移除最后的换行符
        if (response.length() > 0 && response.charAt(response.length() - 1) == '\n') {
            response.setLength(response.length() - 1);
        }

        return response.toString();
    }

    // 构建带查询参数的URL
    private static String buildUrlWithParams(String url, Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return url;
        }

        StringBuilder query = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (query.length() > 0) {
                query.append('&');
            }
            query.append(encode(entry.getKey())).append('=').append(encode(entry.getValue()));
        }

        String baseUrl = url.contains("?") ? url + "&" : url + "?";
        return baseUrl + query.toString();
    }

    // 构建表单请求体
    private static String buildFormData(Map<String, String> formData) {
        if (formData == null || formData.isEmpty()) {
            return "";
        }

        StringBuilder body = new StringBuilder();
        for (Map.Entry<String, String> entry : formData.entrySet()) {
            if (body.length() > 0) {
                body.append('&');
            }
            body.append(encode(entry.getKey())).append('=').append(encode(entry.getValue()));
        }
        return body.toString();
    }

    // URL编码工具方法
    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            throw new RuntimeException("Failed to encode URL parameter: " + value, e);
        }
    }

    public static String sendHttp09Request(String host, int port, String data) throws IOException {

        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(10000); // 10秒超时

            // 使用OutputStream直接发送，避免PrintWriter的自动换行问题
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // 构建HTTP/0.9请求
            // 注意：真正的HTTP/0.9格式非常简单，可能不需要所有头部
            String request = "POST /\r\n" +
                    "Content-Type: application/json\r\n" +
                    "Content-Length: " + data.length() + "\r\n" +
                    "\r\n" +
                    data;

            // 发送请求
            out.write(request.getBytes(StandardCharsets.UTF_8));
            out.flush();

            // 读取响应
            ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;

            // 设置一个简单的超时机制
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 5000) { // 5秒超时
                if (in.available() > 0) {
                    bytesRead = in.read(buffer);
                    if (bytesRead > 0) {
                        responseBuffer.write(buffer, 0, bytesRead);
                    }
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            return responseBuffer.toString(StandardCharsets.UTF_8.name());
        }
    }
}
