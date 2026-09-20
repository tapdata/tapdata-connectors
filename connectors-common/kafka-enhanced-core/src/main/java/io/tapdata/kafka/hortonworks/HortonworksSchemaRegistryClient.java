package io.tapdata.kafka.hortonworks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

/**
 * Hortonworks Schema Registry 适配器（最小实现）
 * 仅实现 TapData 实际用到的方法，其他方法抛 UnsupportedOperationException
 */
public class HortonworksSchemaRegistryClient implements SchemaRegistryClient {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
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
        String schemaStr = schema.canonicalString();
        ensureSchemaMetadata(subject, schema.schemaType());

        String url = baseUrls.get(0) + "/api/v1/schemaregistry/schemas/" + encodePath(subject) + "/versions?branch=MASTER&disableCanonicalCheck=true";
        String requestBody = OBJECT_MAPPER.createObjectNode()
                .put("description", "TapData generated schema")
                .put("schemaText", schemaStr)
                .toString();

        JsonNode body = executeJson(new Request.Builder()
                .url(url)
                .post(RequestBody.create(requestBody, MediaType.parse("application/json"))), true);
        return body.path("id").asInt();
    }

    private void ensureSchemaMetadata(String subject, String schemaType) throws IOException, RestClientException {
        String url = baseUrls.get(0) + "/api/v1/schemaregistry/schemas";
        String requestBody = OBJECT_MAPPER.createObjectNode()
                .put("type", StringUtils.defaultIfBlank(schemaType, "json").toLowerCase(Locale.ROOT))
                .put("schemaGroup", "Kafka")
                .put("name", subject)
                .put("description", "TapData generated schema")
                .put("compatibility", "NONE")
                .put("validationLevel", "ALL")
                .toString();

        executeJson(new Request.Builder()
                .url(url)
                .post(RequestBody.create(requestBody, MediaType.parse("application/json"))), false);
    }

    private JsonNode executeJson(Request.Builder builder, boolean failOnConflict) throws IOException, RestClientException {
        if (basicAuthHeader != null) {
            builder.header("Authorization", basicAuthHeader);
        }

        try (Response response = httpClient.newCall(builder.build()).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                if (!failOnConflict && (response.code() == 400 || response.code() == 409)) {
                    return OBJECT_MAPPER.createObjectNode();
                }
                throw new RestClientException("Hortonworks schema registry request failed: " + response.code() + " " + body,
                        response.code(), 50001);
            }
            if (StringUtils.isBlank(body)) {
                return OBJECT_MAPPER.createObjectNode();
            }
            return OBJECT_MAPPER.readTree(body);
        }
    }

    private String encodePath(String value) {
        return okhttp3.HttpUrl.parse("http://localhost/").newBuilder().addPathSegment(value).build().encodedPath().substring(1);
    }

    @Override
    public SchemaMetadata getLatestSchemaMetadata(String subject) throws IOException, RestClientException {
        String url = baseUrls.get(0) + "/api/v1/schemaregistry/schemas/" + encodePath(subject) + "/versions/latest";
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
            JsonNode json = OBJECT_MAPPER.readTree(body);
            return new SchemaMetadata(json.path("id").asInt(), json.path("version").asInt(), json.path("schemaText").asText());
        }
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
        // Hortonworks Schema Registry does not expose the same compatibility
        // endpoint as Confluent. Treat this as best-effort so topic creation is
        // not blocked; schema registration still happens through register().
        return compatibility;
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


    @Override
    public void reset() {
        // No-op for Hortonworks adapter (stateless HTTP client)
    }

    @Override
    public Optional<ParsedSchema> parseSchema(String schemaType, String schemaString, List<SchemaReference> references) {
        throw new UnsupportedOperationException("parseSchema not implemented for Hortonworks");
    }

    @Override
    public int register(String subject, ParsedSchema schema, int version, int id) throws IOException, RestClientException {
        throw new UnsupportedOperationException("register with version/id not implemented for Hortonworks");
    }

    @Override
    public Collection<String> getAllSubjectsById(int id) throws IOException, RestClientException {
        throw new UnsupportedOperationException("getAllSubjectsById not implemented for Hortonworks");
    }

    @Override
    public SchemaMetadata getSchemaMetadata(String subject, int version) throws IOException, RestClientException {
        throw new UnsupportedOperationException("getSchemaMetadata not implemented for Hortonworks");
    }

    @Override
    public boolean testCompatibility(String subject, ParsedSchema schema) throws IOException, RestClientException {
        throw new UnsupportedOperationException("testCompatibility not implemented for Hortonworks");
    }

    @Override
    public String setMode(String mode) throws IOException, RestClientException {
        throw new UnsupportedOperationException("setMode not implemented for Hortonworks");
    }

    @Override
    public String setMode(String mode, String subject) throws IOException, RestClientException {
        throw new UnsupportedOperationException("setMode with subject not implemented for Hortonworks");
    }

    @Override
    public String getMode() throws IOException, RestClientException {
        throw new UnsupportedOperationException("getMode not implemented for Hortonworks");
    }

    @Override
    public String getMode(String subject) throws IOException, RestClientException {
        throw new UnsupportedOperationException("getMode with subject not implemented for Hortonworks");
    }

    @Override
    public List<Integer> deleteSubject(String subject) throws IOException, RestClientException {
        throw new UnsupportedOperationException("deleteSubject not implemented for Hortonworks");
    }

    @Override
    public List<Integer> deleteSubject(Map<String, String> requestProperties, String subject) throws IOException, RestClientException {
        throw new UnsupportedOperationException("deleteSubject with properties not implemented for Hortonworks");
    }

    @Override
    public Integer deleteSchemaVersion(String subject, String version) throws IOException, RestClientException {
        throw new UnsupportedOperationException("deleteSchemaVersion not implemented for Hortonworks");
    }

    @Override
    public Integer deleteSchemaVersion(Map<String, String> requestProperties, String subject, String version) throws IOException, RestClientException {
        throw new UnsupportedOperationException("deleteSchemaVersion with properties not implemented for Hortonworks");
    }
}
