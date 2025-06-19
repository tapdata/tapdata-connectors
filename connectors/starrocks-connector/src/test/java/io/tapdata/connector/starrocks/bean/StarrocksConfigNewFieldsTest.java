package io.tapdata.connector.starrocks.bean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 测试 StarrocksConfig 新增字段
 */
public class StarrocksConfigNewFieldsTest {

    @Test
    @DisplayName("测试新增配置字段的默认值")
    public void testDefaultValues() {
        StarrocksConfig config = new StarrocksConfig();
        
        // 测试默认值
        assertEquals(100, config.getFlushSizeMB());
        assertEquals(300, config.getFlushTimeoutSeconds());
        assertEquals(0, config.getHourlyLimitGB());
    }

    @Test
    @DisplayName("测试新增配置字段的设置和获取")
    public void testSettersAndGetters() {
        StarrocksConfig config = new StarrocksConfig();
        
        // 测试设置和获取
        config.setFlushSizeMB(200);
        config.setFlushTimeoutSeconds(600);
        config.setHourlyLimitGB(10);
        
        assertEquals(200, config.getFlushSizeMB());
        assertEquals(600, config.getFlushTimeoutSeconds());
        assertEquals(10, config.getHourlyLimitGB());
    }

    @Test
    @DisplayName("测试通过Map加载配置")
    public void testLoadFromMap() {
        StarrocksConfig config = new StarrocksConfig();
        Map<String, Object> configMap = new HashMap<>();
        
        configMap.put("flushSizeMB", 150);
        configMap.put("flushTimeoutSeconds", 450);
        configMap.put("hourlyLimitGB", 5);
        configMap.put("host", "localhost");
        configMap.put("port", 9030);
        configMap.put("database", "test_db");
        
        StarrocksConfig loadedConfig = config.load(configMap);
        
        assertEquals(150, loadedConfig.getFlushSizeMB());
        assertEquals(450, loadedConfig.getFlushTimeoutSeconds());
        assertEquals(5, loadedConfig.getHourlyLimitGB());
    }
}
