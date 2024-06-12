package io.tapdata.connector.tidb.cdc.process;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TiDataTest {
    @Test
    void testNormal() {
        TiData tiData = new TiData();
        tiData.setTableVersion(0L);
        Assertions.assertEquals(0L, tiData.getTableVersion());
    }
}