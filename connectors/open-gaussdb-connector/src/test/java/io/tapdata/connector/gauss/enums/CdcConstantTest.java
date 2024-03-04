package io.tapdata.connector.gauss.enums;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CdcConstantTest {
    @Test
    public void testParams() {
        Assertions.assertEquals("H", CdcConstant.HEART_TAG);
        Assertions.assertEquals("B", CdcConstant.BEGIN_TAG);
        Assertions.assertEquals("I", CdcConstant.INSERT_TAG);
        Assertions.assertEquals("U", CdcConstant.UPDATE_TAG);
        Assertions.assertEquals("D", CdcConstant.DELETE_TAG);
        Assertions.assertEquals("C", CdcConstant.COMMIT_TAG);

        Assertions.assertEquals(1000, CdcConstant.CDC_MAX_BATCH_SIZE);
        Assertions.assertEquals(100, CdcConstant.CDC_DEFAULT_BATCH_SIZE);
        Assertions.assertEquals(0, CdcConstant.CDC_MIN_BATCH_SIZE);

        Assertions.assertEquals(4, CdcConstant.BYTES_COUNT_BUFF_START);
        Assertions.assertEquals(8, CdcConstant.BYTES_COUNT_BUFF_START);
        Assertions.assertEquals(1, CdcConstant.BYTES_COUNT_LSN);

        Assertions.assertEquals(0xFFFFFFFF, CdcConstant.BYTES_VALUE_OF_NULL);
        Assertions.assertEquals(0, CdcConstant.BYTES_VALUE_OF_EMPTY_CHAR);

        Assertions.assertEquals("open_gauss_slot", CdcConstant.GAUSS_DB_SLOT_TAG);
        Assertions.assertEquals("gauss_slot_", CdcConstant.GAUSS_DB_SLOT_SFF);
        Assertions.assertEquals("mppdb_decoding", CdcConstant.GAUSS_DB_SLOT_DEFAULT_PLUGIN);

        Assertions.assertEquals(10 * 60 * 1000, CdcConstant.CDC_FLUSH_LOGIC_LSN_DEFAULT);
        Assertions.assertEquals(10 * 60 * 1000, CdcConstant.CDC_FLUSH_LOGIC_LSN_MIN);
        Assertions.assertEquals(100 * 60 * 1000, CdcConstant.CDC_FLUSH_LOGIC_LSN_MAX);
    }
}
