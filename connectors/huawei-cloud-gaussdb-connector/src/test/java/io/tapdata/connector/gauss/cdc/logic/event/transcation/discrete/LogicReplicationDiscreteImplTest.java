package io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.gauss.cdc.CdcOffset;
import io.tapdata.connector.gauss.cdc.logic.AnalyzeLog;
import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.event.EventFactory;
import io.tapdata.connector.gauss.cdc.logic.event.dml.DeleteEvent;
import io.tapdata.connector.gauss.cdc.logic.event.dml.InsertEvent;
import io.tapdata.connector.gauss.cdc.logic.event.dml.UpdateEvent;
import io.tapdata.connector.gauss.cdc.logic.event.other.HeartBeatEvent;
import io.tapdata.connector.gauss.entity.IllegalDataLengthException;
import io.tapdata.connector.gauss.enums.CdcConstant;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LogicReplicationDiscreteImplTest {
    LogicReplicationDiscreteImpl logic;
    @BeforeEach
    void init() {
        logic = mock(LogicReplicationDiscreteImpl.class);
    }

    @Nested
    class InstanceTest {
        StreamReadConsumer consumer;
        int batchSize;
        CdcOffset offset;
        Supplier<Boolean> supplier;
        @BeforeEach
        void init() {
            consumer = mock(StreamReadConsumer.class);
            batchSize = 100;
            offset = mock(CdcOffset.class);
            supplier = mock(Supplier.class);
        }

        void assertVerify(StreamReadConsumer c, int b, CdcOffset o, Supplier<Boolean> s) {
            try (MockedStatic<LogicReplicationDiscreteImpl> impl = mockStatic(LogicReplicationDiscreteImpl.class)) {
                impl.when(() -> {
                    LogicReplicationDiscreteImpl.instance(c, b, o, s);
                }).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> {
                    EventFactory<ByteBuffer> instance = LogicReplicationDiscreteImpl.instance(c, b, o, s);
                    Assertions.assertNotNull(instance);
                    Assertions.assertEquals(LogicReplicationDiscreteImpl.class.getName(), instance.getClass().getName());
                    LogicReplicationDiscreteImpl i = (LogicReplicationDiscreteImpl)instance;
                    Assertions.assertNotNull(i.offset);
                    Assertions.assertNotNull(i.param);
                    Assertions.assertTrue(i.batchSize <= 1000, "LogicReplicationDiscreteImpl's batch size should Less than and equals CdcConstant.CDC_MAX_BATCH_SIZE(1000)");
                    Assertions.assertTrue(i.batchSize > 0, "LogicReplicationDiscreteImpl's batch size should more than CdcConstant.CDC_MIN_BATCH_SIZE(0)");
                });
            }
        }

        @Test
        void testNormal() {
            assertVerify(consumer, 100, offset, supplier);
        }

        @Test
        void testBatchSizeMoreThanMaxValue() {
            assertVerify(consumer, 100000, offset, supplier);
        }
        @Test
        void testBatchSizeLessThanMinValue() {
            assertVerify(consumer, -100, offset, supplier);
        }
        @Test
        void testOffsetIsNull() {
            assertVerify(consumer, 100, null, supplier);
        }
        @Test
        void testOffsetNotNull() {
            assertVerify(consumer, 100, offset, supplier);
        }
    }

    @Nested
    class HasNextTest {
        ByteBuffer buffer;
        Supplier<Boolean> supplier;
        @BeforeEach
        void init() {
            buffer = mock(ByteBuffer.class);
            supplier = mock(Supplier.class);

            when(logic.hasNext(buffer)).thenCallRealMethod();
            when(logic.hasNext(null)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            logic.supplier = supplier;
            when(supplier.get()).thenReturn(true);
            when(buffer.hasRemaining()).thenReturn(true);
            boolean hasNext = logic.hasNext(buffer);
            Assertions.assertTrue(hasNext);
            verify(supplier, times(1)).get();
            verify(buffer, times(1)).hasRemaining();
        }

        @Test
        void testSupplierIsNull() {
            logic.supplier = null;
            when(supplier.get()).thenReturn(true);
            when(buffer.hasRemaining()).thenReturn(true);
            boolean hasNext = logic.hasNext(buffer);
            Assertions.assertFalse(hasNext);
            verify(supplier, times(0)).get();
            verify(buffer, times(0)).hasRemaining();
        }

        @Test
        void testSupplierNotNullButGetIsFalse() {
            logic.supplier = supplier;
            when(supplier.get()).thenReturn(false);
            when(buffer.hasRemaining()).thenReturn(true);
            boolean hasNext = logic.hasNext(buffer);
            Assertions.assertFalse(hasNext);
            verify(supplier, times(1)).get();
            verify(buffer, times(0)).hasRemaining();
        }

        @Test
        void testSupplierNotNullAndGetIsTrueButBufferIsNull() {
            logic.supplier = supplier;
            when(supplier.get()).thenReturn(true);
            when(buffer.hasRemaining()).thenReturn(true);
            boolean hasNext = logic.hasNext(null);
            Assertions.assertFalse(hasNext);
            verify(supplier, times(1)).get();
            verify(buffer, times(0)).hasRemaining();
        }

        @Test
        void testSupplierNotNullAndGetIsTrueAndBufferNotNullButHasRemainingIsFalse() {
            logic.supplier = supplier;
            when(supplier.get()).thenReturn(true);
            when(buffer.hasRemaining()).thenReturn(false);
            boolean hasNext = logic.hasNext(buffer);
            Assertions.assertFalse(hasNext);
            verify(supplier, times(1)).get();
            verify(buffer, times(1)).hasRemaining();
        }
    }

    @Nested
    class EmitTest {
        ByteBuffer logEvent;
        Log log;
        Event.EventEntity<TapEvent> eventEntity;
        TapEvent event;
        StreamReadConsumer eventAccept;
        CdcOffset offset;

        boolean hasNextTag = true;
        @BeforeEach
        void init() {
            offset = mock(CdcOffset.class);
            doNothing().when(offset).setLsn(anyLong());
            when(offset.getTransactionTimestamp()).thenReturn(1L);

            eventAccept = mock(StreamReadConsumer.class);
            doNothing().when(eventAccept).accept(anyList(), any(CdcOffset.class));

            logEvent = mock(ByteBuffer.class);
            when(logEvent.array()).thenReturn(new byte[0]);
            log = mock(Log.class);
            doNothing().when(log).warn(anyString(), anyString(), anyString());
            when(logic.hasNext(logEvent)).then(a -> {
                if (hasNextTag) {
                    hasNextTag = false;
                    return true;
                }
                return false;
            });
            eventEntity = mock(Event.EventEntity.class);
            when(logic.redirect(any(ByteBuffer.class), anyString())).thenReturn(eventEntity);
            event = mock(TapEvent.class);
            when(eventEntity.event()).thenReturn(event);
            logic.batchSize = 1;
            logic.offset = offset;
            logic.eventAccept = eventAccept;
            doCallRealMethod().when(logic).emit(logEvent, log);
            doCallRealMethod().when(logic).emit(null, log);
        }

        void assertVerify(ByteBuffer b,
                          int allLength, String type,
                          int hasNextTimes,
                          int readBufferStartTimes, int readLsnTimes, int readCountEventTimes, int byteToIntTimes, int byteToLongTimes,
                          int redirectTimes,
                          int eventEntityTimes,
                          int listTimes, int acceptTimes) {
            try (MockedStatic<LogicUtil> u = mockStatic(LogicUtil.class);
                 MockedStatic<ConnectorBase> cb = mockStatic(ConnectorBase.class)) {
                u.when(() -> { LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_BUFF_START); }).thenReturn(new byte[0]);
                u.when(() -> { LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_LSN); }).thenReturn(new byte[0]);
                u.when(() -> { LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_EVENT_TYPE); }).thenReturn(type.getBytes());
                u.when(() -> { LogicUtil.bytesToInt(any(byte[].class)); }).thenReturn(allLength);
                u.when(() -> { LogicUtil.byteToLong(any(byte[].class)); }).thenReturn(1L);
                cb.when(() -> ConnectorBase.list(any(TapEvent.class))).thenReturn(new ArrayList<>());

                logic.emit(b, log);
                u.verify(() -> { LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_BUFF_START); }, times(readBufferStartTimes));
                u.verify(() -> { LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_LSN); }, times(readLsnTimes));
                u.verify(() -> { LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_EVENT_TYPE); }, times(readCountEventTimes));
                u.verify(() -> { LogicUtil.bytesToInt(any(byte[].class)); }, times(byteToIntTimes));
                u.verify(() -> { LogicUtil.byteToLong(any(byte[].class)); }, times(byteToLongTimes));
                cb.verify(() -> ConnectorBase.list(any(TapEvent.class)), times(listTimes));
            } finally {
                verify(logic, times(hasNextTimes)).hasNext(logEvent);
                verify(logic, times(redirectTimes)).redirect(any(ByteBuffer.class), anyString());
                verify(eventEntity, times(eventEntityTimes)).event();
                verify(offset, times(byteToLongTimes)).setLsn(anyLong());
                verify(eventAccept, times(acceptTimes)).accept(anyList(), any(CdcOffset.class));
            }
        }

        @Test
        void testNullByteBuffer() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(null,
                        0,"",
                        0,
                        0, 0, 0, 0, 0,
                        0, 0, 0, 0);
            });
        }
        @Test
        void testAllLengthLessThanZero() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(logEvent,
                        0,"",
                        2,
                        1, 0, 0, 1, 0,
                        0, 0, 0, 0);
            });
        }
        @Test
        void testEventEntityIsNull() {
            when(logic.redirect(any(ByteBuffer.class), anyString())).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(logEvent,
                        10,"Null",
                        2,
                        1, 1, 1, 1, 0,
                        1, 0, 0, 0);
            });
        }
        @Test
        void testTapEventIsNull() {
            when(eventEntity.event()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(logEvent,
                        10,"NotNull",
                        2,
                        1, 1, 1, 1, 0,
                        1, 1, 0, 0);
            });
        }
        @Test
        void testTapEventIsHeartbeatEvent() {
            HeartbeatEvent hb = mock(HeartbeatEvent.class);
            when(eventEntity.event()).thenReturn(hb);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(logEvent,
                        10,"NotNull",
                        1,
                        1, 1, 1, 1, 0,
                        1, 1, 1, 1);
            });
        }
        @Test
        void testTapEventNotTapRecordEvent() {
            TapAlterFieldNameEvent afn = mock(TapAlterFieldNameEvent.class);
            when(eventEntity.event()).thenReturn(afn);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(logEvent,
                        10,"NotNull",
                        2,
                        1, 1, 1, 1, 1,
                        1, 1, 0, 1);
            });
        }
        @Test
        void testEventListSizeNotEqualsBatchSize() {
            logic.batchSize = 10;
            TapRecordEvent tr = mock(TapRecordEvent.class);
            when(eventEntity.event()).thenReturn(tr);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(logEvent,
                        10,"NotNull",
                        2,
                        1, 1, 1, 1, 1,
                        1, 1, 0, 1);
            });
            verify(tr, times(1)).setReferenceTime(anyLong());
            verify(offset, times(1)).getTransactionTimestamp();
        }
        @Test
        void testException() {
            when(logic.redirect(any(ByteBuffer.class), anyString())).thenAnswer(a -> {
                throw new Exception("Error");
            });
            Assertions.assertThrows(Exception.class, () -> {
                assertVerify(logEvent,
                        10,"NotNull",
                        1,
                        1, 1, 1, 1, 0,
                        1, 0, 0, 0);
            });
        }
        @Test
        void testExceptionIsIllegalDataLengthException() {
            when(logic.redirect(any(ByteBuffer.class), anyString())).thenAnswer(a -> {
                throw new IllegalDataLengthException("Error");
            });
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(logEvent,
                        10,"NotNull",
                        1,
                        1, 1, 1, 1, 0,
                        1, 0, 0, 0);
            });
            verify(log, times(1)).warn(anyString(), anyString(),anyString());
        }
    }

    @Nested
    class ProcessTest {
        @Test
        void testNormal() {
            doCallRealMethod().when(logic).process();
            Assertions.assertThrows(UnsupportedOperationException.class, () -> {
                logic.process();
            });
        }
    }

    @Nested
    class RedirectTest {
        ByteBuffer logEvent;
        String type;
        BeginTransaction beginTransaction;
        InsertEvent insertEvent;
        UpdateEvent updateEvent;
        DeleteEvent deleteEvent;
        CommitTransaction commitTransaction;
        HeartBeatEvent heartBeatEvent;
        CdcOffset cdcOffset;
        AnalyzeLog.AnalyzeParam param;
        Event.EventEntity<TapEvent> event;

        @BeforeEach
        void init() {
            event = mock(Event.EventEntity.class);
            when(event.csn()).thenReturn(1L);

            param = mock(AnalyzeLog.AnalyzeParam.class);
            logEvent = mock(ByteBuffer.class);
            type = "";

            beginTransaction = mock(BeginTransaction.class);
            when(beginTransaction.analyze(logEvent, param)).thenReturn(event);

            insertEvent = mock(InsertEvent.class);
            when(insertEvent.analyze(logEvent, param)).thenReturn(event);

            updateEvent = mock(UpdateEvent.class);
            when(updateEvent.analyze(logEvent, param)).thenReturn(event);

            deleteEvent = mock(DeleteEvent.class);
            when(deleteEvent.analyze(logEvent, param)).thenReturn(event);

            commitTransaction = mock(CommitTransaction.class);
            when(commitTransaction.analyze(logEvent, param)).thenReturn(event);

            heartBeatEvent = mock(HeartBeatEvent.class);
            when(heartBeatEvent.analyze(logEvent, param)).thenReturn(event);

            cdcOffset = mock(CdcOffset.class);
            when(cdcOffset.withXidIndex(anyInt())).thenReturn(cdcOffset);
            doNothing().when(cdcOffset).setLsn(anyLong());
            doNothing().when(cdcOffset).setLsn(null);
            when(cdcOffset.withTransactionTimestamp(anyLong())).thenReturn(cdcOffset);

            when(logic.advanceTransactionOffset()).thenReturn(false);

            logic.offset = cdcOffset;
            logic.param = param;
        }

        Event.EventEntity<TapEvent> assertVerify(ByteBuffer l, String t,
                                                 int bTimes, int iTimes, int uTimes, int dTimes, int cTimes, int hTimes,
                                                 int withXidIndexTimes, int setLsnNullTimes, int setLsnTimes, int withTransactionTimestampTimes,
                                                 int advanceTransactionOffsetTimes) {
            AtomicReference<Event.EventEntity<TapEvent>> redirect = new AtomicReference<>();
            Assertions.assertDoesNotThrow(() -> {
                try (MockedStatic<BeginTransaction> b = mockStatic(BeginTransaction.class);
                     MockedStatic<InsertEvent> i = mockStatic(InsertEvent.class);
                     MockedStatic<UpdateEvent> u = mockStatic(UpdateEvent.class);
                     MockedStatic<DeleteEvent> d = mockStatic(DeleteEvent.class);
                     MockedStatic<HeartBeatEvent> h = mockStatic(HeartBeatEvent.class);
                     MockedStatic<CommitTransaction> c = mockStatic(CommitTransaction.class)) {
                    b.when(BeginTransaction::instance).thenReturn(beginTransaction);
                    i.when(InsertEvent::instance).thenReturn(insertEvent);
                    u.when(UpdateEvent::instance).thenReturn(updateEvent);
                    d.when(DeleteEvent::instance).thenReturn(deleteEvent);
                    h.when(HeartBeatEvent::instance).thenReturn(heartBeatEvent);
                    c.when(CommitTransaction::instance).thenReturn(commitTransaction);
                    redirect.set(logic.redirect(l, t));
                    b.verify(BeginTransaction::instance, times(bTimes));
                    i.verify(InsertEvent::instance, times(iTimes));
                    d.verify(DeleteEvent::instance, times(dTimes));
                    u.verify(UpdateEvent::instance, times(uTimes));
                    c.verify(CommitTransaction::instance, times(cTimes));
                    h.verify(HeartBeatEvent::instance, times(hTimes));
                } finally {
                    verify(event, times(bTimes)).csn();
                    verify(cdcOffset, times(withXidIndexTimes)).withXidIndex(anyInt());
                    verify(cdcOffset, times(setLsnTimes)).setLsn(anyLong());
                    verify(cdcOffset, times(setLsnNullTimes)).setLsn(null);
                    verify(cdcOffset, times(withTransactionTimestampTimes)).withTransactionTimestamp(anyLong());
                    verify(beginTransaction, times(bTimes)).analyze(logEvent, param);
                    verify(insertEvent, times(iTimes)).analyze(logEvent, param);
                    verify(updateEvent, times(uTimes)).analyze(logEvent, param);
                    verify(deleteEvent, times(dTimes)).analyze(logEvent, param);
                    verify(heartBeatEvent, times(hTimes)).analyze(logEvent, param);
                    verify(commitTransaction, times(cTimes)).analyze(logEvent, param);
                    verify(logic, times(advanceTransactionOffsetTimes)).advanceTransactionOffset();
                }
            });
            return redirect.get();
        }

        @Test
        void testLogEventIsNull() {
            when(logic.redirect(null, type)).thenCallRealMethod();
            Event.EventEntity<TapEvent> entity = assertVerify(null, type,
                    0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0,
                    0);
            Assertions.assertNull(entity);
        }
        @Test
        void testTypeIsNull() {
            when(logic.redirect(logEvent, null)).thenCallRealMethod();
            Event.EventEntity<TapEvent> entity = assertVerify(logEvent, null,
                    0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0,
                    0);
            Assertions.assertNull(entity);
        }
        @Test
        void testBothLogEventAndTypeAreNull() {
            when(logic.redirect(null, null)).thenCallRealMethod();
            Event.EventEntity<TapEvent> entity = assertVerify(null, null,
                    0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0,
                    0);
            Assertions.assertNull(entity);
        }
        @Test
        void testTypeIsB() {
            type = "B";
            when(logic.redirect(logEvent, type)).thenCallRealMethod();
            Event.EventEntity<TapEvent> entity = assertVerify(logEvent, type,
                    1, 0, 0, 0, 0, 0,
                    1, 0, 1, 1,
                    0);
            Assertions.assertNotNull(entity);
        }

        @Test
        void testTypeIsI() {
            type = "I";
            when(logic.redirect(logEvent, type)).thenCallRealMethod();
            Event.EventEntity<TapEvent> entity = assertVerify(logEvent, type,
                    0, 1, 0, 0, 0, 0,
                    0, 0, 0, 0,
                    1);
            Assertions.assertNotNull(entity);
        }
        @Test
        void testTypeIsU() {
            type = "U";
            when(logic.redirect(logEvent, type)).thenCallRealMethod();
            Event.EventEntity<TapEvent> entity = assertVerify(logEvent, type,
                    0, 0, 1, 0, 0, 0,
                    0, 0, 0, 0,
                    1);
            Assertions.assertNotNull(entity);
        }
        @Test
        void testTypeIsD() {
            type = "D";
            when(logic.redirect(logEvent, type)).thenCallRealMethod();
            Event.EventEntity<TapEvent> entity = assertVerify(logEvent, type,
                    0, 0, 0, 1, 0, 0,
                    0, 0, 0, 0,
                    1);
            Assertions.assertNotNull(entity);
        }
        @Test
        void testTypeC() {
            type = "C";
            when(logic.redirect(logEvent, type)).thenCallRealMethod();
            Event.EventEntity<TapEvent> entity = assertVerify(logEvent, type,
                    0, 0, 0, 0, 1, 0,
                    1, 0, 0, 0,
                    0);
            Assertions.assertNotNull(entity);
        }
        @Test
        void testTypeH() {
            type = "H";
            when(logic.redirect(logEvent, type)).thenCallRealMethod();
            Event.EventEntity<TapEvent> entity = assertVerify(logEvent, type,
                    0, 0, 0, 0, 0, 1,
                    0, 0, 0, 0,
                    0);
            Assertions.assertNotNull(entity);
        }
        @Test
        void testOtherType() {
            type = "M";
            when(logic.redirect(logEvent, type)).thenCallRealMethod();
            Event.EventEntity<TapEvent> entity = assertVerify(logEvent, type,
                    0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0,
                    0);
            Assertions.assertNull(entity);
        }
    }

    @Nested
    class AcceptTest {
        @Test
        void testNormal() {
            doCallRealMethod().when(logic).accept();
            Assertions.assertThrows(UnsupportedOperationException.class, logic::accept);
        }
    }

    @Nested
    class AdvanceTransactionOffsetTest {
        int transactionIndex;
        CdcOffset offset;
        @BeforeEach
        void init() {
            transactionIndex = 0;
            offset = mock(CdcOffset.class);
            when(offset.withXidIndex(anyInt())).thenReturn(offset);

            logic.offset = offset;
            logic.transactionIndex = transactionIndex;
            when(logic.advanceTransactionOffset()).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            when(offset.getXidIndex()).thenReturn(0);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    boolean transactionOffset = logic.advanceTransactionOffset();
                    Assertions.assertTrue(transactionOffset);
                } finally {
                    verify(offset, times(1)).getXidIndex();
                    verify(offset, times(1)).withXidIndex(anyInt());
                }
            });
        }

        @Test
        void testTransactionIndexLessThanXidIndex() {
            logic.transactionIndex = 1;
            when(offset.getXidIndex()).thenReturn(10);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    boolean transactionOffset = logic.advanceTransactionOffset();
                    Assertions.assertFalse(transactionOffset);
                } finally {
                    verify(offset, times(1)).getXidIndex();
                    verify(offset, times(0)).withXidIndex(anyInt());
                }
            });
        }
        @Test
        void testTransactionIndexMoreThanAndEqualsXidIndex() {
            logic.transactionIndex = 100;
            when(offset.getXidIndex()).thenReturn(10);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    boolean transactionOffset = logic.advanceTransactionOffset();
                    Assertions.assertTrue(transactionOffset);
                } finally {
                    verify(offset, times(1)).getXidIndex();
                    verify(offset, times(1)).withXidIndex(anyInt());
                }
            });
        }
    }
}
