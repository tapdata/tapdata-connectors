package io.tapdata.connector.paimon;

import io.tapdata.connector.paimon.config.PaimonConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for PaimonConfig
 */
public class PaimonConfigTest {

    @Test
    public void testLocalStorageWarehousePath() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setStorageType("local");
        
        String fullPath = config.getFullWarehousePath();
        assertEquals("file:///tmp/paimon", fullPath);
    }

    @Test
    public void testS3StorageWarehousePath() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("bucket/warehouse");
        config.setStorageType("s3");
        
        String fullPath = config.getFullWarehousePath();
        assertEquals("s3a://bucket/warehouse", fullPath);
    }

    @Test
    public void testHdfsStorageWarehousePath() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/warehouse");
        config.setStorageType("hdfs");
        config.setHdfsHost("namenode");
        config.setHdfsPort(9000);
        
        String fullPath = config.getFullWarehousePath();
        assertEquals("hdfs://namenode:9000/warehouse", fullPath);
    }

    @Test
    public void testValidateS3Config() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("bucket/warehouse");
        config.setStorageType("s3");
        config.setS3Endpoint("https://s3.amazonaws.com");
        config.setS3AccessKey("access-key");
        config.setS3SecretKey("secret-key");
        
        assertDoesNotThrow(() -> config.validate());
    }

    @Test
    public void testValidateS3ConfigMissingEndpoint() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("bucket/warehouse");
        config.setStorageType("s3");
        config.setS3AccessKey("access-key");
        config.setS3SecretKey("secret-key");
        
        Exception exception = assertThrows(IllegalArgumentException.class, () -> config.validate());
        assertTrue(exception.getMessage().contains("S3 endpoint"));
    }

    @Test
    public void testValidateEmptyWarehouse() {
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("local");
        
        Exception exception = assertThrows(IllegalArgumentException.class, () -> config.validate());
        assertTrue(exception.getMessage().contains("Warehouse path"));
    }

    @Test
    public void testValidateUnsupportedStorageType() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setStorageType("unsupported");
        
        Exception exception = assertThrows(IllegalArgumentException.class, () -> config.validate());
        assertTrue(exception.getMessage().contains("Unsupported storage type"));
    }
}

