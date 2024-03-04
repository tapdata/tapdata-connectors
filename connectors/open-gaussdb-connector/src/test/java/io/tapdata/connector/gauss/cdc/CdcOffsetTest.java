package io.tapdata.connector.gauss.cdc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        offset.withXidIndex(10);
    }

    @Test
    void testWithTransactionTimestamp() {
        when(offset.withTransactionTimestamp(anyLong())).thenCallRealMethod();
        doNothing().when(offset).setTransactionTimestamp(anyLong());
        offset.withTransactionTimestamp(10L);
    }

    @Test
    void testGetXidIndex() {
        when(offset.getXidIndex()).thenCallRealMethod();
        int xidIndex = offset.getXidIndex();
    }
    @Test
    void testSetXidIndex() {
        doCallRealMethod().when(offset).setXidIndex(anyInt());
        offset.setXidIndex(10);
    }
    @Test
    void testGetLsn() {
        when(offset.getLsn()).thenCallRealMethod();
        Object lsn = offset.getLsn();
    }
    @Test
    void testSetLsn() {
        doCallRealMethod().when(offset).setLsn(anyLong());
        offset.setLsn(10L);
    }
    @Test
    void testGetTransactionTimestamp() {
        when(offset.getTransactionTimestamp()).thenCallRealMethod();
        long transactionTimestamp = offset.getTransactionTimestamp();
    }
    @Test
    void testSetTransactionTimestamp() {
        doCallRealMethod().when(offset).setTransactionTimestamp(anyLong());
        offset.setTransactionTimestamp(10L);
    }
}
