package io.tapdata.oceanbase.bean;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CdcMemoryLimitTest {
    @Test
    void shouldDefaultAndNormalizeMemoryLimit() {
        OceanbaseConfig config = new OceanbaseConfig();
        assertEquals("2G", config.getCdcMemoryLimit());
        for (String value : new String[] {null, "", "  "}) {
            config.setCdcMemoryLimit(value);
            assertEquals("2G", config.getCdcMemoryLimit());
        }
        // Exercise normalization directly. The inherited config load path uses
        // the legacy bean mapper, whose CGLIB dependency fails on JDKs that do
        // not open java.lang to unnamed modules.
        config.setCdcMemoryLimit("512m");
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
