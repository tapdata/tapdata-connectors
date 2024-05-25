package io.tapdata.connector.gauss.cdc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CdcOffsetTest {
    CdcOffset offset;
    @BeforeEach
    void init() {
        offset = mock(CdcOffset.class);
    }

    @Test
    void testWithXidIndex() {
        when(offset.withXidIndex(anyInt())).thenCallRealMethod();
        doNothing().when(offset).setXidIndex(anyInt());
        Assertions.assertDoesNotThrow(() -> offset.withXidIndex(10));
    }

    @Test
    void testWithTransactionTimestamp() {
        when(offset.withTransactionTimestamp(anyLong())).thenCallRealMethod();
        doNothing().when(offset).setTransactionTimestamp(anyLong());
        Assertions.assertDoesNotThrow(() -> offset.withTransactionTimestamp(10L));
    }

    @Test
    void testGetXidIndex() {
        when(offset.getXidIndex()).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> offset.getXidIndex());
    }
    @Test
    void testSetXidIndex() {
        doCallRealMethod().when(offset).setXidIndex(anyInt());
        Assertions.assertDoesNotThrow(() -> offset.setXidIndex(10));
    }
    @Test
    void testGetLsn() {
        when(offset.getLsn()).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> offset.getLsn());
    }
    @Test
    void testSetLsn() {
        doCallRealMethod().when(offset).setLsn(anyLong());
        Assertions.assertDoesNotThrow(() -> offset.setLsn(10L));
    }
    @Test
    void testGetTransactionTimestamp() {
        when(offset.getTransactionTimestamp()).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> offset.getTransactionTimestamp());
    }
    @Test
    void testSetTransactionTimestamp() {
        doCallRealMethod().when(offset).setTransactionTimestamp(anyLong());
        Assertions.assertDoesNotThrow(() -> offset.setTransactionTimestamp(10L));
    }

    @Test
    void testMap() {
        CdcOffset offset = CdcOffset.fromOffset(new HashMap<>());
        Assertions.assertEquals(0L, offset.getXidIndex());
        Assertions.assertEquals(0, offset.getXidIndex());
    }

    @Test
    void testMap1() {
        HashMap<Object, Object> map = new HashMap<>();
        map.put(CdcOffset.XID_INDEX, 11);
        CdcOffset offset = CdcOffset.fromOffset(map);
        Assertions.assertEquals(11, offset.getXidIndex());
    }
    @Test
    void testMap11() {
        HashMap<Object, Object> map = new HashMap<>();
        map.put(CdcOffset.XID_INDEX, "");
        CdcOffset offset = CdcOffset.fromOffset(map);
        Assertions.assertEquals(0, offset.getXidIndex());
    }

    @Test
    void testMap2() {
        HashMap<Object, Object> map = new HashMap<>();
        map.put(CdcOffset.LSN, 11L);
        CdcOffset offset = CdcOffset.fromOffset(map);
        Assertions.assertEquals(0, offset.getXidIndex());
        Assertions.assertEquals(11L, offset.getLsn());
        Assertions.assertEquals(0, offset.getXidIndex());
    }
    @Test
    void testMap21() {
        HashMap<Object, Object> map = new HashMap<>();
        map.put(CdcOffset.LSN, "11L");
        CdcOffset offset = CdcOffset.fromOffset(map);
        Assertions.assertEquals(0, offset.getXidIndex());
        Assertions.assertNull(offset.getLsn());
    }
    @Test
    void testMap3() {
        HashMap<Object, Object> map = new HashMap<>();
        map.put(CdcOffset.TRANSACTION_TIMESTAMP, 11L);
        CdcOffset offset = CdcOffset.fromOffset(map);
        Assertions.assertEquals(0, offset.getXidIndex());
        Assertions.assertEquals(0L, offset.getXidIndex());
    }
    @Test
    void testMap31() {
        HashMap<Object, Object> map = new HashMap<>();
        map.put(CdcOffset.TRANSACTION_TIMESTAMP, "11L");
        CdcOffset offset = CdcOffset.fromOffset(map);
        Assertions.assertEquals(0, offset.getXidIndex());
        Assertions.assertEquals(0L, offset.getXidIndex());
    }
}
