package io.tapdata.mongodb;


import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.mongodb.batch.ErrorHandler;
import io.tapdata.mongodb.batch.MongoBatchReader;
import io.tapdata.mongodb.entity.MongoCdcOffset;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.entity.ReadParam;
import io.tapdata.mongodb.reader.MongodbOpLogStreamV3Reader;
import io.tapdata.mongodb.reader.MongodbStreamReader;
import io.tapdata.mongodb.reader.StreamWithOpLogCollection;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MongodbConnectorTest {
    MongodbConnector mongodbConnector;
    TapConnectorContext connectorContext;
    MongodbConfig mongoConfig;
    MongodbStreamReader mongodbStreamReader;
    MongodbStreamReader opLogStreamReader;
    MongodbExceptionCollector exceptionCollector;
    ThreadPoolExecutor sourceRunner;
    Future<?> sourceRunnerFuture;
    Log log;
    @BeforeEach
    void init() {
        mongodbConnector = mock(MongodbConnector.class);
        connectorContext = mock(TapConnectorContext.class);
        mongoConfig = mock(MongodbConfig.class);
        ReflectionTestUtils.setField(mongodbConnector, "mongoConfig", mongoConfig);
        mongodbStreamReader = mock(MongodbStreamReader.class);
        ReflectionTestUtils.setField(mongodbConnector, "mongodbStreamReader", mongodbStreamReader);
        opLogStreamReader = mock(MongodbStreamReader.class);
        ReflectionTestUtils.setField(mongodbConnector, "opLogStreamReader", opLogStreamReader);
        exceptionCollector = new MongodbExceptionCollector();
        ReflectionTestUtils.setField(mongodbConnector, "exceptionCollector", exceptionCollector);
        sourceRunner = mock(ThreadPoolExecutor.class);
        ReflectionTestUtils.setField(mongodbConnector, "sourceRunner", sourceRunner);
        sourceRunnerFuture = mock(Future.class);
        ReflectionTestUtils.setField(mongodbConnector, "sourceRunnerFuture", sourceRunnerFuture);
        log = mock(Log.class);
        when(connectorContext.getLog()).thenReturn(log);
    }

    @Nested
    class BatchReadTest {
        TapTable table;
        Object offset;
        int eventBatchSize;
        BiConsumer<List<TapEvent>, Object> tapReadOffsetConsumer;
        MongoBatchReader reader;
        Exception e;
        @BeforeEach
        void init() throws Throwable {
            e = new Exception();
            reader = mock(MongoBatchReader.class);
            table = mock(TapTable.class);
            offset = 0L;
            eventBatchSize = 100;
            tapReadOffsetConsumer = mock(BiConsumer.class);
            doNothing().when(mongodbConnector).errorHandle(e, connectorContext);
            doAnswer(a -> {
                ReadParam param = a.getArgument(0, ReadParam.class);
                ErrorHandler errorHandler = param.getErrorHandler();
                errorHandler.doHandle(e);
                return null;
            }).when(reader).batchReadCollection(any(ReadParam.class));
            doCallRealMethod().when(mongodbConnector).batchRead(connectorContext, table, offset, eventBatchSize, tapReadOffsetConsumer);
        }
        @Test
        void testNormal() throws Throwable {
            try(MockedStatic<MongoBatchReader> mbr = mockStatic(MongoBatchReader.class)) {
                mbr.when(() -> MongoBatchReader.of(any(ReadParam.class))).thenReturn(reader);
                mongodbConnector.batchRead(connectorContext, table, offset, eventBatchSize, tapReadOffsetConsumer);
                verify(mongodbConnector).errorHandle(e, connectorContext);
            }
        }
    }

    @Nested
    class StreamOffsetTest {
        Long offsetStartTime;

        @BeforeEach
        void init() {
            offsetStartTime = 0L;

            when(mongoConfig.getDatabase()).thenReturn("test");
            when(opLogStreamReader.streamOffset(offsetStartTime)).thenReturn(0L);
            when(mongodbStreamReader.streamOffset(offsetStartTime)).thenReturn(0L);
            when(mongodbConnector.createStreamReader()).thenReturn(mongodbStreamReader);
            doNothing().when(opLogStreamReader).onStart(mongoConfig);
            when(mongodbConnector.streamOffset(connectorContext, offsetStartTime)).thenCallRealMethod();
        }
        @Test
        void testNormal() {
            Object offset = mongodbConnector.streamOffset(connectorContext, offsetStartTime);
            Assertions.assertNotNull(offset);
            Assertions.assertEquals(HashMap.class.getName(), offset.getClass().getName());
            MongoCdcOffset o = MongoCdcOffset.fromOffset(offset);
            Assertions.assertEquals(0L, o.getOpLogOffset());
            Assertions.assertEquals(0L, o.getCdcOffset());
            verify(opLogStreamReader).streamOffset(offsetStartTime);
            verify(mongoConfig, times(0)).getDatabase();
            verify(mongodbStreamReader).streamOffset(offsetStartTime);
        }
        @Test
        void testMongodbStreamReaderIsNull() {
            ReflectionTestUtils.setField(mongodbConnector, "mongodbStreamReader", null);
            when(mongodbConnector.createStreamReader()).thenReturn(mongodbStreamReader);
            Object offset = mongodbConnector.streamOffset(connectorContext, offsetStartTime);
            Assertions.assertNotNull(offset);
            Assertions.assertEquals(HashMap.class.getName(), offset.getClass().getName());
            MongoCdcOffset o = MongoCdcOffset.fromOffset(offset);
            Assertions.assertEquals(0L, o.getOpLogOffset());
            Assertions.assertEquals(0L, o.getCdcOffset());
            verify(opLogStreamReader).streamOffset(offsetStartTime);
            verify(mongodbStreamReader).streamOffset(offsetStartTime);
            verify(mongoConfig, times(0)).getDatabase();
            verify(mongodbConnector).createStreamReader();
        }
        @Test
        void testOpLogStreamReaderIsNullButDatabaseNotLocal() {
            ReflectionTestUtils.setField(mongodbConnector, "opLogStreamReader", null);
            Object offset = mongodbConnector.streamOffset(connectorContext, offsetStartTime);
            Assertions.assertNotNull(offset);
            Assertions.assertEquals(HashMap.class.getName(), offset.getClass().getName());
            MongoCdcOffset o = MongoCdcOffset.fromOffset(offset);
            Assertions.assertNull(o.getOpLogOffset());
            Assertions.assertEquals(0L, o.getCdcOffset());
            verify(opLogStreamReader, times(0)).streamOffset(offsetStartTime);
            verify(mongodbStreamReader).streamOffset(offsetStartTime);
            verify(mongoConfig).getDatabase();
            verify(mongodbConnector, times(0)).createStreamReader();
        }
        @Test
        void testOpLogStreamReaderIsNullDatabaseIsLocal() {
            when(mongoConfig.getDatabase()).thenReturn("local");
            ReflectionTestUtils.setField(mongodbConnector, "opLogStreamReader", null);
            try(MockedStatic<MongodbOpLogStreamV3Reader> mol = mockStatic(MongodbOpLogStreamV3Reader.class)) {
                mol.when(MongodbOpLogStreamV3Reader::of).thenReturn(opLogStreamReader);
                Object offset = mongodbConnector.streamOffset(connectorContext, offsetStartTime);
                Assertions.assertNotNull(offset);
                Assertions.assertEquals(HashMap.class.getName(), offset.getClass().getName());
                MongoCdcOffset o = MongoCdcOffset.fromOffset(offset);
                Assertions.assertEquals(0L, o.getOpLogOffset());
                Assertions.assertEquals(0L, o.getCdcOffset());
                verify(opLogStreamReader).streamOffset(offsetStartTime);
                verify(mongodbStreamReader).streamOffset(offsetStartTime);
                verify(mongoConfig).getDatabase();
                verify(mongodbConnector, times(0)).createStreamReader();
                verify(opLogStreamReader).onStart(mongoConfig);
            }
        }
    }

    @Nested
    class StreamReadTest {
        List<String> tableList;
        Object offset;
        int eventBatchSize;
        StreamReadConsumer consumer;
        MongoCdcOffset mongoCdcOffset;

        @BeforeEach
        void init() {
            mongoCdcOffset = new MongoCdcOffset(0L, 0L);
            tableList = mock(List.class);
            offset = 0L;
            eventBatchSize = 100;
            consumer = mock(StreamReadConsumer.class);
            when(tableList.size()).thenReturn(1, 1);

            doNothing().when(mongodbConnector).streamReadOpLog(connectorContext, tableList, mongoCdcOffset.getOpLogOffset(), eventBatchSize, consumer);
            when(tableList.isEmpty()).thenReturn(false);
            when(mongodbConnector.createStreamReader()).thenReturn(mongodbStreamReader);
            doNothing().when(mongodbConnector).doStreamRead(mongodbStreamReader, connectorContext, tableList, 0L, eventBatchSize, consumer);
            doCallRealMethod().when(mongodbConnector).streamRead(connectorContext, tableList, offset, eventBatchSize, consumer);
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> mongodbConnector.streamRead(connectorContext, tableList, offset, eventBatchSize, consumer));
            verify(tableList, times(2)).size();
            verify(mongodbConnector).streamReadOpLog(connectorContext, tableList, null, eventBatchSize, consumer);
            verify(mongodbConnector, times(0)).streamReadOpLog(connectorContext, tableList, mongoCdcOffset.getOpLogOffset(), eventBatchSize, consumer);
            verify(tableList, times(0)).isEmpty();
            verify(mongodbConnector, times(0)).createStreamReader();
            verify(mongodbConnector).doStreamRead(mongodbStreamReader, connectorContext, tableList, 0L, eventBatchSize, consumer);
        }
        @Test
        void testAfterReadOpLog() {
            when(tableList.size()).thenReturn(1, 2);
            Assertions.assertDoesNotThrow(() -> mongodbConnector.streamRead(connectorContext, tableList, offset, eventBatchSize, consumer));
            verify(tableList, times(2)).size();
            verify(mongodbConnector).streamReadOpLog(connectorContext, tableList, null, eventBatchSize, consumer);
            verify(mongodbConnector, times(0)).streamReadOpLog(connectorContext, tableList, mongoCdcOffset.getOpLogOffset(), eventBatchSize, consumer);
            verify(tableList).isEmpty();
            verify(mongodbConnector, times(0)).createStreamReader();
            verify(mongodbConnector).doStreamRead(mongodbStreamReader, connectorContext, tableList, 0L, eventBatchSize, consumer);
        }
        @Test
        void testMongodbStreamReaderIsNull() {
            ReflectionTestUtils.setField(mongodbConnector, "mongodbStreamReader", null);
            Assertions.assertDoesNotThrow(() -> mongodbConnector.streamRead(connectorContext, tableList, offset, eventBatchSize, consumer));
            verify(tableList, times(2)).size();
            verify(mongodbConnector).streamReadOpLog(connectorContext, tableList, null, eventBatchSize, consumer);
            verify(mongodbConnector, times(0)).streamReadOpLog(connectorContext, tableList, mongoCdcOffset.getOpLogOffset(), eventBatchSize, consumer);
            verify(tableList, times(0)).isEmpty();
            verify(mongodbConnector, times(1)).createStreamReader();
            verify(mongodbConnector).doStreamRead(mongodbStreamReader, connectorContext, tableList, 0L, eventBatchSize, consumer);
        }
        @Test
        void testOffsetIsMongoOffset() {
            offset = mongoCdcOffset;
            doCallRealMethod().when(mongodbConnector).streamRead(connectorContext, tableList, offset, eventBatchSize, consumer);
            Assertions.assertDoesNotThrow(() -> mongodbConnector.streamRead(connectorContext, tableList, offset, eventBatchSize, consumer));
            verify(tableList, times(2)).size();
            verify(mongodbConnector, times(0)).streamReadOpLog(connectorContext, tableList, null, eventBatchSize, consumer);
            verify(mongodbConnector).streamReadOpLog(connectorContext, tableList, mongoCdcOffset.getOpLogOffset(), eventBatchSize, consumer);
            verify(tableList, times(0)).isEmpty();
            verify(mongodbConnector, times(0)).createStreamReader();
            verify(mongodbConnector).doStreamRead(mongodbStreamReader, connectorContext, tableList, 0L, eventBatchSize, consumer);
        }
        @Test
        void testOnlyReadOpLogCollection() {
            when(tableList.size()).thenReturn(1, 2);
            when(tableList.isEmpty()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> mongodbConnector.streamRead(connectorContext, tableList, offset, eventBatchSize, consumer));
            verify(tableList, times(2)).size();
            verify(mongodbConnector).streamReadOpLog(connectorContext, tableList, null, eventBatchSize, consumer);
            verify(mongodbConnector, times(0)).streamReadOpLog(connectorContext, tableList, mongoCdcOffset.getOpLogOffset(), eventBatchSize, consumer);
            verify(tableList).isEmpty();
            verify(mongodbConnector, times(0)).createStreamReader();
            verify(mongodbConnector, times(0)).doStreamRead(mongodbStreamReader, connectorContext, tableList, 0L, eventBatchSize, consumer);
        }
    }

    @Nested
    class DoStreamTest {
        List<String> tableList;
        Object offset;
        int eventBatchSize;
        StreamReadConsumer consumer;
        MongoCdcOffset mongoCdcOffset;
        MongodbStreamReader streamReader;
        @BeforeEach
        void init() throws Exception {
            streamReader = mock(MongodbStreamReader.class);
            mongoCdcOffset = new MongoCdcOffset(0L, 0L);
            tableList = mock(List.class);
            offset = 0L;
            eventBatchSize = 100;
            consumer = mock(StreamReadConsumer.class);
            when(tableList.size()).thenReturn(1, 1);

            doNothing().when(streamReader).read(connectorContext, tableList, offset, eventBatchSize, consumer);
            doNothing().when(streamReader).onDestroy();
            doNothing().when(mongodbConnector).errorHandle(any(Exception.class), any(TapConnectorContext.class));
            doNothing().when(log).debug(anyString(), anyString());

            doCallRealMethod().when(mongodbConnector).doStreamRead(streamReader, connectorContext, tableList, offset, eventBatchSize, consumer);
        }
        @Test
        void testNormal() throws Exception {
            Assertions.assertDoesNotThrow(() -> mongodbConnector.doStreamRead(streamReader, connectorContext, tableList, offset, eventBatchSize, consumer));
            verify(streamReader, times(1)).read(connectorContext, tableList, offset, eventBatchSize, consumer);
            verify(streamReader, times(0)).onDestroy();
            verify(mongodbConnector, times(0)).errorHandle(any(Exception.class), any(TapConnectorContext.class));
            verify(log, times(0)).debug(anyString(), anyString());
        }
        @Test
        void testException() throws Exception {
            doAnswer(a -> {
                throw new Exception("error");
            }).when(streamReader).read(connectorContext, tableList, offset, eventBatchSize, consumer);
            Assertions.assertDoesNotThrow(() -> mongodbConnector.doStreamRead(streamReader, connectorContext, tableList, offset, eventBatchSize, consumer));
            verify(streamReader, times(1)).read(connectorContext, tableList, offset, eventBatchSize, consumer);
            verify(streamReader, times(1)).onDestroy();
            verify(mongodbConnector, times(1)).errorHandle(any(Exception.class), any(TapConnectorContext.class));
            verify(log, times(0)).debug(anyString(), anyString());
        }
        @Test
        void testDestroyException() throws Exception {
            doAnswer(a -> {
                throw new Exception("error");
            }).when(streamReader).read(connectorContext, tableList, offset, eventBatchSize, consumer);
            doAnswer(a -> {
                throw new Exception("failed");
            }).when(streamReader).onDestroy();
            Assertions.assertDoesNotThrow(() -> mongodbConnector.doStreamRead(streamReader, connectorContext, tableList, offset, eventBatchSize, consumer));
            verify(streamReader, times(1)).read(connectorContext, tableList, offset, eventBatchSize, consumer);
            verify(streamReader, times(1)).onDestroy();
            verify(mongodbConnector, times(1)).errorHandle(any(Exception.class), any(TapConnectorContext.class));
            verify(log, times(1)).debug(anyString(), anyString());
        }
    }

    @Nested
    class StreamReadOpLogTest {
        List<String> tableList;
        Object offset;
        int eventBatchSize;
        StreamReadConsumer consumer;
        MongoCdcOffset mongoCdcOffset;
        @BeforeEach
        void init() {
            mongoCdcOffset = new MongoCdcOffset(0L, 0L);
            tableList = mock(List.class);
            offset = 0L;
            eventBatchSize = 100;
            consumer = mock(StreamReadConsumer.class);

            when(mongoConfig.getDatabase()).thenReturn("test");
            when(tableList.contains(StreamWithOpLogCollection.OP_LOG_COLLECTION)).thenReturn(false);
            doNothing().when(log).info("Start read oplog collection, db: local");
            when(tableList.remove(StreamWithOpLogCollection.OP_LOG_COLLECTION)).thenReturn(true);
            when(tableList.isEmpty()).thenReturn(true);
            doNothing().when(mongodbConnector).doStreamRead(
                    any(MongodbStreamReader.class),
                    any(TapConnectorContext.class),
                    anyList(),
                    any(),
                    anyInt(),
                    any(StreamReadConsumer.class));
            doCallRealMethod().when(mongodbConnector).streamReadOpLog(connectorContext, tableList, 0L, eventBatchSize, consumer);
        }

        @Test
        void testNotReadOpLog() {
            Assertions.assertDoesNotThrow(() -> mongodbConnector.streamReadOpLog(connectorContext, tableList, 0L, eventBatchSize, consumer));
            verify(mongoConfig).getDatabase();
            verify(tableList, times(0)).contains(StreamWithOpLogCollection.OP_LOG_COLLECTION);
            verify(connectorContext, times(0)).getLog();
            verify(log, times(0)).info("Start read oplog collection, db: local");
            verify(tableList, times(0)).remove(StreamWithOpLogCollection.OP_LOG_COLLECTION);
            verify(tableList, times(0)).isEmpty();
            verify(mongodbConnector, times(0)).doStreamRead(
                    any(MongodbStreamReader.class),
                    any(TapConnectorContext.class),
                    anyList(),
                    any(),
                    anyInt(),
                    any(StreamReadConsumer.class));
        }
        @Test
        void testNotReadOpLog2() {
            when(mongoConfig.getDatabase()).thenReturn("local");
            Assertions.assertDoesNotThrow(() -> mongodbConnector.streamReadOpLog(connectorContext, tableList, 0L, eventBatchSize, consumer));
            verify(mongoConfig).getDatabase();
            verify(tableList, times(1)).contains(StreamWithOpLogCollection.OP_LOG_COLLECTION);
            verify(connectorContext, times(0)).getLog();
            verify(log, times(0)).info("Start read oplog collection, db: local");
            verify(tableList, times(0)).remove(StreamWithOpLogCollection.OP_LOG_COLLECTION);
            verify(tableList, times(0)).isEmpty();
            verify(mongodbConnector, times(0)).doStreamRead(
                    any(MongodbStreamReader.class),
                    any(TapConnectorContext.class),
                    anyList(),
                    any(),
                    anyInt(),
                    any(StreamReadConsumer.class));
        }
        @Test
        void testReadOpLog() {
            when(mongoConfig.getDatabase()).thenReturn("local");
            when(tableList.contains(StreamWithOpLogCollection.OP_LOG_COLLECTION)).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> mongodbConnector.streamReadOpLog(connectorContext, tableList, 0L, eventBatchSize, consumer));
            verify(mongoConfig).getDatabase();
            verify(tableList, times(1)).contains(StreamWithOpLogCollection.OP_LOG_COLLECTION);
            verify(connectorContext, times(1)).getLog();
            verify(log, times(1)).info("Start read oplog collection, db: local");
            verify(tableList, times(1)).remove(StreamWithOpLogCollection.OP_LOG_COLLECTION);
            verify(tableList, times(1)).isEmpty();
            verify(mongodbConnector, times(1)).doStreamRead(
                    any(MongodbStreamReader.class),
                    any(TapConnectorContext.class),
                    anyList(),
                    any(),
                    anyInt(),
                    any(StreamReadConsumer.class));
        }
        @Test
        void testReadOpLogWithNullMongodbOpLogStreamV3Reader() {
            try (MockedStatic<MongodbOpLogStreamV3Reader> mol = mockStatic(MongodbOpLogStreamV3Reader.class)) {
                mol.when(MongodbOpLogStreamV3Reader::of).thenReturn(opLogStreamReader);
                doNothing().when(opLogStreamReader).onStart(mongoConfig);
                ReflectionTestUtils.setField(mongodbConnector, "opLogStreamReader", null);
                when(mongoConfig.getDatabase()).thenReturn("local");
                when(tableList.contains(StreamWithOpLogCollection.OP_LOG_COLLECTION)).thenReturn(true);
                Assertions.assertDoesNotThrow(() -> mongodbConnector.streamReadOpLog(connectorContext, tableList, 0L, eventBatchSize, consumer));
                verify(mongoConfig).getDatabase();
                verify(tableList, times(1)).contains(StreamWithOpLogCollection.OP_LOG_COLLECTION);
                verify(connectorContext, times(1)).getLog();
                verify(log, times(1)).info("Start read oplog collection, db: local");
                verify(tableList, times(1)).remove(StreamWithOpLogCollection.OP_LOG_COLLECTION);
                verify(tableList, times(1)).isEmpty();
                verify(opLogStreamReader).onStart(mongoConfig);
                verify(mongodbConnector, times(1)).doStreamRead(
                        any(MongodbStreamReader.class),
                        any(TapConnectorContext.class),
                        anyList(),
                        any(),
                        anyInt(),
                        any(StreamReadConsumer.class));
            }
        }
    }

    @Nested
    class CloseOpLogThreadSourceTest {

        @BeforeEach
        void init() {
            when(sourceRunnerFuture.cancel(true)).thenReturn(true);

            doNothing().when(sourceRunner).shutdown();
            doCallRealMethod().when(mongodbConnector).closeOpLogThreadSource();
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(mongodbConnector::closeOpLogThreadSource);
            verify(sourceRunnerFuture).cancel(true);
            verify(sourceRunner).shutdown();
            Assertions.assertNull(mongodbConnector.sourceRunner);
            Assertions.assertNull(mongodbConnector.sourceRunnerFuture);
        }
        @Test
        void testThrowException() {
            when(sourceRunnerFuture.cancel(true)).thenAnswer(a -> {
                throw new Exception("failed");
            });
            doAnswer(a -> {
                throw new Exception("failed");
            }).when(sourceRunner).shutdown();
            Assertions.assertDoesNotThrow(mongodbConnector::closeOpLogThreadSource);
            verify(sourceRunnerFuture).cancel(true);
            verify(sourceRunner).shutdown();
            Assertions.assertNull(mongodbConnector.sourceRunner);
            Assertions.assertNull(mongodbConnector.sourceRunnerFuture);
        }
    }
    @Nested
    class OnstartTest{
        @Test
        void testOnstartWithEx() throws Throwable {
            when(connectorContext.getConnectionConfig()).thenReturn(mock(DataMap.class));
            doCallRealMethod().when(mongodbConnector).onStart(connectorContext);
            assertThrows(RuntimeException.class, ()->mongodbConnector.onStart(connectorContext));
        }
    }
}