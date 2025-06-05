package io.tapdata.kit;

import java.io.IOException;
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
}
