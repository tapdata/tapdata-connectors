package io.tapdata.kafka.utils;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Author:Skeet
 * Date: 2023/6/30
 **/
public class SchemaRegisterUtil {

    public static Response sendBasicAuthRequest(String url, String username, String password) throws IOException {
        String user = username + ":" + password;
        String encodedCredentials = Base64.getEncoder().encodeToString(user.getBytes(StandardCharsets.UTF_8));
        OkHttpClient client = new OkHttpClient().newBuilder().build();
        Request request = new Request.Builder()
                .url(url)
                .method("GET", null)
                .header("Authorization", "Basic " + encodedCredentials)
                .build();
        try {
            Response response = client.newCall(request).execute();
            response.close();
            return response;
        } catch (IOException e) {
            throw new IOException(e.getMessage());
        }
    }

    public static List<String> parseJsonArray(String jsonArray) {
        List<String> result = new ArrayList<>();
        jsonArray = jsonArray.trim();
        if (jsonArray.startsWith("[") && jsonArray.endsWith("]")) {
            jsonArray = jsonArray.substring(1, jsonArray.length() - 1);
            String[] elements = jsonArray.split(",");
            for (String element : elements) {
                String subject = element.trim().replaceAll("\"", "");
                result.add(subject);
            }
        }
        return result;
    }

}
