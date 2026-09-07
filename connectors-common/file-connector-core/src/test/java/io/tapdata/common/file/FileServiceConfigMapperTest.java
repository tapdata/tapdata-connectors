package io.tapdata.common.file;

import io.tapdata.file.operation.FileEndpoint;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileServiceConfigMapperTest {
    @Test
    void mapsGenericEndpointKeysToFtpStorageKeys() {
        Map<String, Object> params = new HashMap<>();
        params.put("host", "ftp.example.com");
        params.put("port", 21);
        params.put("username", "user");
        FileEndpoint endpoint = FileEndpoint.builder().protocol("ftp").params(params).build();

        Map<String, Object> mapped = new DefaultFileServiceConfigMapper().mapStorageParams(endpoint);

        assertEquals("ftp.example.com", mapped.get("ftpHost"));
        assertEquals(21, mapped.get("ftpPort"));
        assertEquals("user", mapped.get("ftpUsername"));
    }
}
