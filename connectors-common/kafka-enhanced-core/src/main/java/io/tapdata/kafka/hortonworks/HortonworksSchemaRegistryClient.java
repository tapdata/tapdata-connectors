package io.tapdata.kafka.hortonworks;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Hortonworks Schema Registry 适配器（最小实现）
 * 仅实现 TapData 实际用到的方法，其他方法抛 UnsupportedOperationException
 */
public class HortonworksSchemaRegistryClient implements SchemaRegistryClient {
    
    private final List<String> baseUrls;
    private final OkHttpClient httpClient;
    private final String basicAuthHeader;

    public HortonworksSchemaRegistryClient(List<String> baseUrls, Map<String, Object> configs) {
        this.baseUrls = baseUrls;
        this.httpClient = new OkHttpClient.Builder().build();
        
        // 处理 Basic Auth
        if (configs != null && "USER_INFO".equals(configs.get("basic.auth.credentials.source"))) {
            String userInfo = (String) configs.get("basic.auth.user.info");
            if (StringUtils.isNotBlank(userInfo)) {
                String encoded = Base64.getEncoder().encodeToString(userInfo.getBytes(StandardCharsets.UTF_8));
                this.basicAuthHeader = "Basic " + encoded;
            } else {
                this.basicAuthHeader = null;
            }
        } else {
            this.basicAuthHeader = null;
        }
    }

    @Override
    public int register(String subject, ParsedSchema schema) throws IOException, RestClientException {
        // Hortonworks API: POST /subjects/{subject}/versions
        // body: {"schema": "<escaped JSON string>"}  <-- 不带 schemaType
        String url = baseUrls.get(0) + "/subjects/" + subject + "/versions";
        String schemaStr = schema.canonicalString();
        // 需要转义双引号并包装成 JSON 字符串
        String escapedSchema = schemaStr.replace("\\", "\\\\").replace("\"", "\\\"");
        String requestBody = "{\"schema\":\"" + escapedSchema + "\"}";
        
        Request.Builder builder = new Request.Builder()
                .url(url)
                .post(RequestBody.create(requestBody, MediaType.parse("application/json")));
        
        if (basicAuthHeader != null) {
            builder.header("Authorization", basicAuthHeader);
        }
        
        try (Response response = httpClient.newCall(builder.build()).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                throw new RestClientException("Register schema failed: " + response.code() + " " + body, 
                        response.code(), 50001);
            }
            // 解析返回的 {"id": 123}
            return parseId(body);
        }
    }

    private int parseId(String json) {
        // 简单解析 {"id":123}
        int idIndex = json.indexOf("\"id\"");
        if (idIndex < 0) {
            throw new RuntimeException("Invalid response: " + json);
        }
        int colonIndex = json.indexOf(":", idIndex);
        int commaIndex = json.indexOf(",", colonIndex);
        int braceIndex = json.indexOf("}", colonIndex);
        int endIndex = commaIndex > 0 ? Math.min(commaIndex, braceIndex) : braceIndex;
        String idStr = json.substring(colonIndex + 1, endIndex).trim();
        return Integer.parseInt(idStr);
    }

    @Override
    public SchemaMetadata getLatestSchemaMetadata(String subject) throws IOException, RestClientException {
        // Hortonworks API: GET /subjects/{subject}/versions/latest
        String url = baseUrls.get(0) + "/subjects/" + subject + "/versions/latest";
        Request.Builder builder = new Request.Builder().url(url).get();
        if (basicAuthHeader != null) {
            builder.header("Authorization", basicAuthHeader);
        }
        
        try (Response response = httpClient.newCall(builder.build()).execute()) {
            if (response.code() == 404) {
                throw new RestClientException("Schema not found", 404, 40401);
            }
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                throw new RestClientException("Get schema failed: " + response.code() + " " + body,
                        response.code(), 50001);
            }
            // 简化解析: 假设返回 {"id":1, "version":1, "schema":"..."}
            int id = parseFieldInt(body, "id");
            int version = parseFieldInt(body, "version");
            String schema = parseFieldString(body, "schema");
            return new SchemaMetadata(id, version, schema);
        }
    }

    private int parseFieldInt(String json, String field) {
        int fieldIndex = json.indexOf("\"" + field + "\"");
        if (fieldIndex < 0) return -1;
        int colonIndex = json.indexOf(":", fieldIndex);
        int commaIndex = json.indexOf(",", colonIndex);
        int braceIndex = json.indexOf("}", colonIndex);
        int endIndex = commaIndex > 0 ? Math.min(commaIndex, braceIndex) : braceIndex;
        String valueStr = json.substring(colonIndex + 1, endIndex).trim();
        return Integer.parseInt(valueStr);
    }

    private String parseFieldString(String json, String field) {
        int fieldIndex = json.indexOf("\"" + field + "\"");
        if (fieldIndex < 0) return null;
        int colonIndex = json.indexOf(":", fieldIndex);
        int quoteStart = json.indexOf("\"", colonIndex);
        int quoteEnd = json.indexOf("\"", quoteStart + 1);
        return json.substring(quoteStart + 1, quoteEnd);
    }

    // ========== 以下是接口要求但 TapData 未使用的方法，全部抛异常 ==========
    
    @Override
    public List<Integer> getAllVersions(String subject) throws IOException, RestClientException {
        throw new UnsupportedOperationException("Not implemented for Hortonworks");
    }

    @Override
    public int getVersion(String subject, ParsedSchema schema) throws IOException, RestClientException {
        throw new UnsupportedOperationException("Not implemented for Hortonworks");
    }

    @Override
    public String updateCompatibility(String subject, String compatibility) throws IOException, RestClientException {
        // 如果 TapData 用到了这个方法，需要实现
        throw new UnsupportedOperationException("Not implemented for Hortonworks");
    }

    @Override
    public String getCompatibility(String subject) throws IOException, RestClientException {
        throw new UnsupportedOperationException("Not implemented for Hortonworks");
    }

    @Override
    public Collection<String> getAllSubjects() throws IOException, RestClientException {
        throw new UnsupportedOperationException("Not implemented for Hortonworks");
    }

    @Override
    public int getId(String subject, ParsedSchema schema) throws IOException, RestClientException {
        throw new UnsupportedOperationException("Not implemented for Hortonworks");
    }

    @Override
    public ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
        throw new UnsupportedOperationException("Not implemented for Hortonworks");
    }

    @Override
    public ParsedSchema getSchemaBySubjectAndId(String subject, int id) throws IOException, RestClientException {
        throw new UnsupportedOperationException("Not implemented for Hortonworks");
    }

    // 还有更多接口方法，这里省略...
}
