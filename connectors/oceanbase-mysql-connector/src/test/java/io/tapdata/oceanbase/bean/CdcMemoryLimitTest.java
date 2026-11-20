package io.tapdata.oceanbase.bean;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class CdcMemoryLimitTest {
    @Test
    void shouldDefaultAndLoadMemoryLimit() {
        OceanbaseConfig config = new OceanbaseConfig();
        assertEquals("2G", config.getCdcMemoryLimit());
        for (String value : new String[] {null, "", "  "}) {
            config.setCdcMemoryLimit(value);
            assertEquals("2G", config.getCdcMemoryLimit());
        }
        Map<String, Object> values = new HashMap<>();
        values.put("cdcMemoryLimit", "512m");
        config.load(values);
        assertEquals("512M", config.getCdcMemoryLimit());
        config.setCdcMemoryLimit(" 4g ");
        assertEquals("4G", config.getCdcMemoryLimit());
    }

    @Test
    void shouldRejectInvalidOrInjectedMemoryLimit() {
        OceanbaseConfig config = new OceanbaseConfig();
        for (String value : new String[] {"0G", "-1G", "2GB", "1.5G", "2G\ncluster_user=other"}) {
            config.setCdcMemoryLimit(value);
            assertThrows(IllegalArgumentException.class, config::getCdcMemoryLimit);
        }
    }
}
