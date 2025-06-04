package io.tapdata.connector.postgres;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.sqlparser.ResultDO;
import io.tapdata.connector.postgres.cdc.PostgresCDCSQLParser;
import io.tapdata.entity.simplify.TapSimplify;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * HTTP/0.9 客户端 - 使用原始Socket连接
 * 修复了Apache HttpClient无法处理HTTP/0.9协议的问题
 */
public class PgtoParser {

    protected static final PostgresCDCSQLParser sqlParser = new PostgresCDCSQLParser();

    public static void main(String[] args) throws Exception {
        String host = "192.168.1.184";
        int port = 19876;
        String data = "peek";

        System.out.println("尝试连接到HTTP/0.9服务器: " + host + ":" + port);
        System.out.println("发送数据: " + data);
        System.out.println("========================================");

        // 方法1: 使用原始Socket (推荐)
        System.out.println("\n=== 方法1: 使用原始Socket ===");
        try {
            String response1 = sendHttp09Request(host, port, data);
            if (response1 != null && !response1.isEmpty()) {
                System.out.println("✅ Socket方法成功!");
                return; // 成功就退出
            }
        } catch (Exception e) {
            System.err.println("❌ Socket方法失败: " + e.getMessage());
        }

        // 方法2: 使用简化的HTTP/0.9格式
        System.out.println("\n=== 方法2: 使用简化HTTP/0.9格式 ===");
        try {
            String response2 = sendSimpleHttp09Request(host, port, data);
            if (response2 != null && !response2.isEmpty()) {
                System.out.println("✅ 简化HTTP/0.9方法成功!");
                return;
            }
        } catch (Exception e) {
            System.err.println("❌ 简化HTTP/0.9方法失败: " + e.getMessage());
        }

        // 方法3: 使用HttpURLConnection (备用)
        System.out.println("\n=== 方法3: 使用HttpURLConnection ===");
        try {
            String response3 = sendHttpURLConnectionRequest(host, port, data);
            if (response3 != null && !response3.isEmpty()) {
                System.out.println("✅ HttpURLConnection方法成功!");
            }
        } catch (Exception e) {
            System.err.println("❌ HttpURLConnection方法失败: " + e.getMessage());
        }

        // 方法4: 使用纯TCP连接（最后尝试）
        System.out.println("\n=== 方法4: 使用纯TCP连接 ===");
        try {
            String response4 = sendRawTcpRequest(host, port, data);
            if (response4 != null && !response4.isEmpty()) {
                System.out.println("✅ 纯TCP方法成功!");
            }
        } catch (Exception e) {
            System.err.println("❌ 纯TCP方法失败: " + e.getMessage());
        }
    }

    /**
     * 使用原始Socket发送HTTP/0.9请求
     */
    private static String sendHttp09Request(String host, int port, String data) throws IOException {
        System.out.println("连接到: " + host + ":" + port);

        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(10000); // 10秒超时

            // 使用OutputStream直接发送，避免PrintWriter的自动换行问题
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // 构建HTTP/0.9请求
            // 注意：真正的HTTP/0.9格式非常简单，可能不需要所有头部
            StringBuilder request = new StringBuilder();
            request.append("POST /\r\n");
            request.append("Content-Type: application/json\r\n");
            request.append("Content-Length: ").append(data.length()).append("\r\n");
            request.append("\r\n");
            request.append(data);

            System.out.println("发送请求:");
            System.out.println(request.toString().replace("\r\n", "\\r\\n\n"));

            // 发送请求
            out.write(request.toString().getBytes(StandardCharsets.UTF_8));
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

            String response = responseBuffer.toString(StandardCharsets.UTF_8);
            System.out.println("收到响应 (" + response.length() + " 字节):");
            System.out.println(response);
            extractJsonObjects(response).forEach(str -> {
                JSONObject jsonObject = TapSimplify.fromJson(str, JSONObject.class);
                String sql = jsonObject.getString("sql");
                ResultDO redo = sqlParser.from(sql, false);
            });

            return response;
        }
    }

    public static List<String> extractJsonObjects(String input) {
        List<String> jsonObjects = new ArrayList<>();
        StringBuilder jsonBuilder = null;
        int braceCount = 0;
        boolean insideJson = false;

        for (char c : input.toCharArray()) {
            if (c == '{') {
                if (!insideJson) {
                    // 开始新的JSON对象
                    jsonBuilder = new StringBuilder();
                    insideJson = true;
                }
                braceCount++;
            }

            if (insideJson) {
                jsonBuilder.append(c);

                if (c == '}') {
                    braceCount--;
                    // 当所有大括号都闭合时
                    if (braceCount == 0) {
                        jsonObjects.add(jsonBuilder.toString());
                        insideJson = false;
                    }
                }
            }
        }

        return jsonObjects;
    }

    /**
     * 使用更简单的HTTP/0.9格式请求
     */
    private static String sendSimpleHttp09Request(String host, int port, String data) throws IOException {
        System.out.println("尝试简化的HTTP/0.9请求...");

        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(10000);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // 最简单的HTTP/0.9请求格式
            String request = "POST /\r\n\r\n" + data;

            System.out.println("发送简化请求:");
            System.out.println(request.replace("\r\n", "\\r\\n\n"));

            out.write(request.getBytes(StandardCharsets.UTF_8));
            out.flush();

            // 读取响应
            ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;

            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 5000) {
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

            String response = responseBuffer.toString(StandardCharsets.UTF_8);
            System.out.println("收到简化响应:");
            System.out.println(response);

            return response;
        }
    }

    /**
     * 使用HttpURLConnection发送请求 (设置为HTTP/1.0)
     */
    private static String sendHttpURLConnectionRequest(String host, int port, String data) throws IOException {
        java.net.URL url = new java.net.URL("http://" + host + ":" + port);
        java.net.HttpURLConnection connection = (java.net.HttpURLConnection) url.openConnection();

        try {
            // 设置连接属性
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(10000);

            // 设置请求头
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Content-Length", String.valueOf(data.length()));
            connection.setRequestProperty("Connection", "close");

            // 发送数据
            try (OutputStream os = connection.getOutputStream()) {
                os.write(data.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }

            // 读取响应
            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            InputStream inputStream = (responseCode >= 200 && responseCode < 300)
                ? connection.getInputStream()
                : connection.getErrorStream();

            if (inputStream != null) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line).append("\n");
                    }
                    return response.toString().trim();
                }
            }

            return "No response body";

        } finally {
            connection.disconnect();
        }
    }

    /**
     * 使用纯TCP连接发送数据（最简单的方式）
     */
    private static String sendRawTcpRequest(String host, int port, String data) throws IOException {
        System.out.println("尝试纯TCP连接...");

        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(10000);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // 直接发送数据，不使用任何HTTP协议
            System.out.println("发送原始数据: " + data);
            out.write(data.getBytes(StandardCharsets.UTF_8));
            out.flush();

            // 读取响应
            ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;

            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 5000) {
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

            String response = responseBuffer.toString(StandardCharsets.UTF_8);
            System.out.println("收到TCP响应:");
            System.out.println(response);

            return response;
        }
    }
}
