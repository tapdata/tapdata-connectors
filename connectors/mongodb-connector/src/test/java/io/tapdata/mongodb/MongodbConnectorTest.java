package io.tapdata.mongodb;

import com.mongodb.client.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapIndexEx;
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
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
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
    MongoClient mongoClient;
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
        mongoClient = mock(MongoClient.class);
        ReflectionTestUtils.setField(mongodbConnector,"mongoClient",mongoClient);
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
            assertThrows(Exception.class,mongodbConnector::closeOpLogThreadSource);
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
    @Nested
    class getMongoCollection{
        private MongoDatabase mongoDatabase;
        private String table;
        @BeforeEach
        void beforeEach(){
            table = "table";
            mongoDatabase = mock(MongoDatabase.class);
            ReflectionTestUtils.setField(mongodbConnector,"mongoDatabase",mongoDatabase);
        }
        @Test
        void testGetMongoCollectionWithEx(){
            when(mongoDatabase.getCollection(table)).thenThrow(RuntimeException.class);
            when(mongoConfig.getUri()).thenReturn("mongodb://127.0.0.1:27017/test");
            doCallRealMethod().when(mongodbConnector).getMongoCollection(table);
            assertThrows(RuntimeException.class,()->mongodbConnector.getMongoCollection(table));
        }
    }
    @Nested
    class executeCommand{
        private TapExecuteCommand tapExecuteCommand;
        private Consumer<ExecuteResult> executeResultConsumer;
        private MongodbExecuteCommandFunction mongodbExecuteCommandFunction;
        @BeforeEach
        void beforeEach(){
            tapExecuteCommand = mock(TapExecuteCommand.class);
            executeResultConsumer = mock(Consumer.class);
            mongodbExecuteCommandFunction = mock(MongodbExecuteCommandFunction.class);
        }
        @Test
        void testExecuteCommandWithEx(){
            Map<String, Object> executeObj = new HashMap<>();
            executeObj.put("database","test");
            when(tapExecuteCommand.getParams()).thenReturn(executeObj);
            when(tapExecuteCommand.getCommand()).thenReturn("executeQuery");
            doThrow(new RuntimeException()).when(mongodbExecuteCommandFunction).executeQuery(anyMap(),any(MongoClient.class),any(Consumer.class),any(Supplier.class));
            doCallRealMethod().when(mongodbConnector).executeCommand(connectorContext,tapExecuteCommand,executeResultConsumer);
            assertThrows(RuntimeException.class,()->mongodbConnector.executeCommand(connectorContext,tapExecuteCommand,executeResultConsumer));
        }
    }
    @Nested
    class queryFieldMinMaxValue{
        private TapTable table;
        private TapAdvanceFilter partitionFilter;
        private String fieldName;
        @BeforeEach
        void beforeEach(){
            table = mock(TapTable.class);
            partitionFilter = mock(TapAdvanceFilter.class);
            fieldName = "field";
            when(table.getId()).thenReturn("table");
        }
        @Test
        void testQueryFieldMinMaxValueWithEx(){
            MongoCollection<Document> collection = mock(MongoCollection.class);
            when(mongodbConnector.getMongoCollection("table")).thenReturn(collection);
            TapIndexEx partitionIndex = mock(TapIndexEx.class);
            when(table.partitionIndex()).thenReturn(partitionIndex);
            List<TapIndexField> indexFields = new ArrayList<>();
            TapIndexField indexField = mock(TapIndexField.class);
            when(indexField.getName()).thenReturn(fieldName);
            indexFields.add(indexField);
            when(partitionIndex.getIndexFields()).thenReturn(indexFields);
            when(collection.find(any(Bson.class))).thenThrow(RuntimeException.class);
            doCallRealMethod().when(mongodbConnector).queryFieldMinMaxValue(connectorContext,table,partitionFilter,fieldName);
            assertThrows(RuntimeException.class,()->mongodbConnector.queryFieldMinMaxValue(connectorContext,table,partitionFilter,fieldName));
        }
    }
    @Nested
    class batchCount{
        private TapTable table;
        @Test
        void testBatchCountWithEx() throws Throwable {
            try (MockedStatic<MongodbConnector> mb = Mockito
                    .mockStatic(MongodbConnector.class)) {
                table = mock(TapTable.class);
                when(table.getId()).thenReturn("collectionName");
                when(mongoConfig.getDatabase()).thenReturn("database");
                mb.when(()->MongodbConnector.getCollectionNotAggregateCountByTableName(mongoClient,"database","collectionName",null)).thenThrow(RuntimeException.class);
                doCallRealMethod().when(mongodbConnector).batchCount(connectorContext,table);
                assertThrows(RuntimeException.class,()->mongodbConnector.batchCount(connectorContext,table));
            }
        }
    }
    @Nested
    class createIndex{
        private TapTable table;
        private List<TapIndex> indexList;
        private Log log;
        private MongoDatabase mongoDatabase;
        @BeforeEach
        void beforeEach(){
            table = mock(TapTable.class);
            when(table.getName()).thenReturn("table");
            indexList = new ArrayList<>();
            TapIndex tapIndex = new TapIndex();
            tapIndex.setName("__t__test");
            indexList.add(tapIndex);
            log = mock(Log.class);
            mongoDatabase = mock(MongoDatabase.class);
            ReflectionTestUtils.setField(mongodbConnector,"mongoDatabase",mongoDatabase);
        }
        @Test
        void testCreateIndexWithExWhenGetCollection(){
            try (MockedStatic<Document> mb = Mockito
                    .mockStatic(Document.class)) {
                mb.when(()->Document.parse("test")).thenReturn(mock(Document.class));
                when(mongoDatabase.getCollection("table")).thenThrow(RuntimeException.class);
                doCallRealMethod().when(mongodbConnector).createIndex(table,indexList,log);
                mongodbConnector.createIndex(table,indexList,log);
                verify(log).warn(anyString());
            }
        }
        @Test
        void testCreateIndexWithExWhenCreateIndex(){
            try (MockedStatic<Document> mb = Mockito
                    .mockStatic(Document.class)) {
                Document document = mock(Document.class);
                mb.when(()->Document.parse("test")).thenReturn(document);
                MongoCollection<Document> targetCollection = mock(MongoCollection.class);
                when(mongoDatabase.getCollection("table")).thenReturn(targetCollection);
                when(targetCollection.createIndex(any(),any(IndexOptions.class))).thenThrow(RuntimeException.class);
                doCallRealMethod().when(mongodbConnector).createIndex(table,indexList,log);
                mongodbConnector.createIndex(table,indexList,log);
                verify(log).warn(anyString());
            }
        }
    }
    @Nested
    class createIndexWithTapCreateIndexEvent{
        private TapTable table;
        private TapCreateIndexEvent tapCreateIndexEvent;
        private MongoDatabase mongoDatabase;
        @BeforeEach
        void beforeEach(){
            table = mock(TapTable.class);
            when(table.getName()).thenReturn("test");
            tapCreateIndexEvent = mock(TapCreateIndexEvent.class);
            mongoDatabase = mock(MongoDatabase.class);
            ReflectionTestUtils.setField(mongodbConnector,"mongoDatabase",mongoDatabase);
        }
        @Test
        void testCreateIndexEventWithEx(){
            List<TapIndex> indexList = new ArrayList<>();
            TapIndex tapIndex = new TapIndex();
            tapIndex.setName("index");
            List<TapIndexField> indexFields = new ArrayList<>();
            indexFields.add(mock(TapIndexField.class));
            tapIndex.setIndexFields(indexFields);
            indexList.add(tapIndex);
            when(tapCreateIndexEvent.getIndexList()).thenReturn(indexList);
            when(mongoDatabase.getCollection("test")).thenThrow(RuntimeException.class);
            when(mongoConfig.getUri()).thenReturn("mongodb://127.0.0.1:27017/test");
            doCallRealMethod().when(mongodbConnector).createIndex(connectorContext,table,tapCreateIndexEvent);
            assertThrows(RuntimeException.class,()->mongodbConnector.createIndex(connectorContext,table,tapCreateIndexEvent));
        }
    }
    @Nested
    class createStreamReader{

        @Test
        void testCreateStreamReaderWithEx(){
            when(mongoConfig.getDatabase()).thenReturn("database");
            when(mongoClient.getDatabase("database")).thenThrow(RuntimeException.class);
            doCallRealMethod().when(mongodbConnector).createStreamReader();
            assertThrows(RuntimeException.class,()->mongodbConnector.createStreamReader());
        }
    }
    @Nested
    class getTableNames{
        private int batchSize;
        private Consumer<List<String>> listConsumer;
        @Test
        void testGetTableNamesWithEx() throws Throwable {
            when(mongoConfig.getDatabase()).thenReturn("database");
            when(mongoClient.getDatabase("database")).thenThrow(RuntimeException.class);
            doCallRealMethod().when(mongodbConnector).getTableNames(connectorContext,batchSize,listConsumer);
            assertThrows(RuntimeException.class,()->mongodbConnector.getTableNames(connectorContext,batchSize,listConsumer));
        }
    }
    @Nested
    class onStop{
        @Test
        void testOnStopWithEx() throws Throwable {
            doThrow(RuntimeException.class).when(mongoClient).close();
            doCallRealMethod().when(mongodbConnector).onStop(connectorContext);
            assertThrows(RuntimeException.class,()->mongodbConnector.onStop(connectorContext));
        }
    }
    @Nested
    class GetTableInfoTest{
        private TapConnectionContext tapConnectorContext;
        private String tableName;
        @BeforeEach
        void beforeEach(){
            tapConnectorContext = mock(TapConnectorContext.class);
            tableName = "table";
        }
        @Test
        void testGetTableInfoWithEx() throws Throwable {
            when(mongoConfig.getDatabase()).thenReturn("test");
            when(mongoClient.getDatabase("test")).thenThrow(RuntimeException.class);
            doCallRealMethod().when(mongodbConnector).getTableInfo(tapConnectorContext,tableName);
            assertThrows(RuntimeException.class, ()->mongodbConnector.getTableInfo(tapConnectorContext,tableName));
        }
    }

    @Nested
    @DisplayName("Method queryIndexes test")
    class QueryIndexesTest {

        private TapConnectorContext tapConnectorContext;
        private TapTable tapTable;

        @BeforeEach
        void setUp() {
            tapConnectorContext = mock(TapConnectorContext.class);
            tapTable = new TapTable("test");
            doCallRealMethod().when(mongodbConnector).queryIndexes(any(), any(), any());
        }

        @Test
        @DisplayName("test main process")
        void test1() {
            List<Document> listIndexes = new ArrayList<>();
            listIndexes.add(new Document("name", "_id_").append("key", new Document("_id", 1)));
            listIndexes.add(new Document("name", "uid_1").append("key", new Document("uid", 1)));
            listIndexes.add(new Document("name", "sub.sid1_1_sub.sid2_-1").append("key", new Document("sub.sid1", 1).append("sub.sid2", -1)));
            Iterator<Document> iterator = listIndexes.iterator();
            MongoCursor<Document> mongoCursor = mock(MongoCursor.class);
            when(mongoCursor.next()).thenAnswer(invocationOnMock -> iterator.next());
            when(mongoCursor.hasNext()).thenAnswer(invocationOnMock -> iterator.hasNext());
            ListIndexesIterable<Document> listIndexesIterable = mock(ListIndexesIterable.class);
            when(listIndexesIterable.iterator()).thenReturn(mongoCursor);
            doCallRealMethod().when(listIndexesIterable).forEach(any(Consumer.class));
            MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
            when(mongoCollection.listIndexes()).thenReturn(listIndexesIterable);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            when(mongoDatabase.getCollection(tapTable.getId())).thenReturn(mongoCollection);
            ReflectionTestUtils.setField(mongodbConnector, "mongoDatabase", mongoDatabase);
            mongodbConnector.queryIndexes(tapConnectorContext, tapTable, indexes -> {
                assertEquals(3, indexes.size());
                TapIndex tapIndex = indexes.get(0);
                assertEquals(listIndexes.get(0).getString("name"), tapIndex.getName());
                List<TapIndexField> indexFields = tapIndex.getIndexFields();
                assertEquals(1, indexFields.size());
                assertEquals("_id", indexFields.get(0).getName());
                assertTrue(indexFields.get(0).getFieldAsc());
                tapIndex = indexes.get(2);
                assertEquals(listIndexes.get(2).getString("name"), tapIndex.getName());
                indexFields = tapIndex.getIndexFields();
                assertEquals("sub.sid2", indexFields.get(1).getName());
                assertFalse(indexFields.get(1).getFieldAsc());
            });
        }

        @Test
        @DisplayName("test other index key value: text")
        void test2() {
            List<Document> listIndexes = new ArrayList<>();
            listIndexes.add(new Document("name", "content_text").append("key", new Document("content", "text")));
            Iterator<Document> iterator = listIndexes.iterator();
            MongoCursor<Document> mongoCursor = mock(MongoCursor.class);
            when(mongoCursor.next()).thenAnswer(invocationOnMock -> iterator.next());
            when(mongoCursor.hasNext()).thenAnswer(invocationOnMock -> iterator.hasNext());
            ListIndexesIterable<Document> listIndexesIterable = mock(ListIndexesIterable.class);
            when(listIndexesIterable.iterator()).thenReturn(mongoCursor);
            doCallRealMethod().when(listIndexesIterable).forEach(any(Consumer.class));
            MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
            when(mongoCollection.listIndexes()).thenReturn(listIndexesIterable);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            when(mongoDatabase.getCollection(tapTable.getId())).thenReturn(mongoCollection);
            ReflectionTestUtils.setField(mongodbConnector, "mongoDatabase", mongoDatabase);
            mongodbConnector.queryIndexes(tapConnectorContext, tapTable, indexes -> {
                assertEquals(1, indexes.size());
                TapIndex tapIndex = indexes.get(0);
                assertEquals(listIndexes.get(0).getString("name"), tapIndex.getName());
                List<TapIndexField> indexFields = tapIndex.getIndexFields();
                assertEquals(1, indexFields.size());
                assertEquals("content", indexFields.get(0).getName());
                assertTrue(indexFields.get(0).getFieldAsc());
            });
        }

        @Test
        @DisplayName("test unique index")
        void test3() {
            List<Document> listIndexes = new ArrayList<>();
            listIndexes.add(new Document("name", "uid_1").append("key", new Document("uid", 1)).append("unique", true));
            listIndexes.add(new Document("name", "uid1_1").append("key", new Document("uid1", 1)));
            Iterator<Document> iterator = listIndexes.iterator();
            MongoCursor<Document> mongoCursor = mock(MongoCursor.class);
            when(mongoCursor.next()).thenAnswer(invocationOnMock -> iterator.next());
            when(mongoCursor.hasNext()).thenAnswer(invocationOnMock -> iterator.hasNext());
            ListIndexesIterable<Document> listIndexesIterable = mock(ListIndexesIterable.class);
            when(listIndexesIterable.iterator()).thenReturn(mongoCursor);
            doCallRealMethod().when(listIndexesIterable).forEach(any(Consumer.class));
            MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
            when(mongoCollection.listIndexes()).thenReturn(listIndexesIterable);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            when(mongoDatabase.getCollection(tapTable.getId())).thenReturn(mongoCollection);
            ReflectionTestUtils.setField(mongodbConnector, "mongoDatabase", mongoDatabase);
            mongodbConnector.queryIndexes(tapConnectorContext, tapTable, indexes -> {
                assertEquals(2, indexes.size());
                assertTrue(indexes.get(0).getUnique());
                assertFalse(indexes.get(1).getUnique());
            });
        }
    }
}