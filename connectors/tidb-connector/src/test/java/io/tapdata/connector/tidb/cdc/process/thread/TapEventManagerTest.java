package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.connector.tidb.cdc.process.analyse.AnalyseTapEventFromDDLObject;
import io.tapdata.connector.tidb.cdc.process.analyse.AnalyseTapEventFromDMLObject;
import io.tapdata.connector.tidb.cdc.process.ddl.entity.DDLObject;
import io.tapdata.connector.tidb.cdc.process.dml.entity.DMLObject;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TapEventManagerTest {
    TapEventManager manager;

    Log log;
    Map<String, Map<String, Long>> offset;
    AnalyseTapEventFromDMLObject dmlEventParser;
    AnalyseTapEventFromDDLObject ddlEventParser;
    ProcessHandler handler;
    AtomicReference<Throwable> throwableCollector;
    StreamReadConsumer consumer;
    KVReadOnlyMap<TapTable> tableMap;
    @BeforeEach
    void setUp() {
        offset = new HashMap<>();
        dmlEventParser = mock(AnalyseTapEventFromDMLObject.class);
        ddlEventParser = mock(AnalyseTapEventFromDDLObject.class);
        handler = mock(ProcessHandler.class);
        throwableCollector = new AtomicReference<>();
        consumer = mock(StreamReadConsumer.class);
        tableMap = mock(KVReadOnlyMap.class);
        log = mock(Log.class);
        manager = mock(TapEventManager.class);
        ReflectionTestUtils.setField(manager, "log", log);
        ReflectionTestUtils.setField(manager, "offset", offset);
        ReflectionTestUtils.setField(manager, "dmlEventParser", dmlEventParser);
        ReflectionTestUtils.setField(manager, "ddlEventParser", ddlEventParser);
        ReflectionTestUtils.setField(manager, "handler", handler);
        ReflectionTestUtils.setField(manager, "throwableCollector", throwableCollector);
        ReflectionTestUtils.setField(manager, "consumer", consumer);
        ReflectionTestUtils.setField(manager, "tableMap", tableMap);
    }

    @Nested
    class ParseOffsetTest {
        @Test
        void testMap() {
            when(manager.parseOffset(anyMap())).thenCallRealMethod();
            Map<String, Map<String, Long>> stringMapMap = manager.parseOffset(new HashMap<>());
            Assertions.assertNotNull(stringMapMap);
            Assertions.assertEquals(HashMap.class.getName(), stringMapMap.getClass().getName());
        }

        @Test
        void testNull(){
            when(manager.parseOffset(null)).thenCallRealMethod();
            Map<String, Map<String, Long>> stringMapMap = manager.parseOffset(null);
            Assertions.assertNotNull(stringMapMap);
            Assertions.assertEquals(HashMap.class.getName(), stringMapMap.getClass().getName());
        }

        @Test
        void testOther() {
            when(manager.parseOffset(1)).thenCallRealMethod();
            Map<String, Map<String, Long>> stringMapMap = manager.parseOffset(1);
            Assertions.assertNotNull(stringMapMap);
            Assertions.assertEquals(HashMap.class.getName(), stringMapMap.getClass().getName());
        }
    }

    @Nested
    class EmitTest {
        @BeforeEach
        void init() {
            doNothing().when(manager).handleDDL(any(DDLObject.class));
            doNothing().when(manager).handleDML(any(DMLObject.class));
            doNothing().when(log).warn(anyString(), anyString());
        }
        @Test
        void testDDL(){
            doCallRealMethod().when(manager).emit(any(DDLObject.class));
            manager.emit(new DDLObject());
            verify(manager, times(1)).handleDDL(any(DDLObject.class));
            verify(manager, times(0)).handleDML(any(DMLObject.class));
            verify(log, times(0)).warn(anyString(), anyString());
        }
        @Test
        void testDML(){
            doCallRealMethod().when(manager).emit(any(DMLObject.class));
            manager.emit(new DMLObject());
            verify(manager, times(0)).handleDDL(any(DDLObject.class));
            verify(manager, times(1)).handleDML(any(DMLObject.class));
            verify(log, times(0)).warn(anyString(), anyString());
        }
        @Test
        void testOther() {
            doCallRealMethod().when(manager).emit(null);
            manager.emit(null);
            verify(manager, times(0)).handleDDL(any(DDLObject.class));
            verify(manager, times(0)).handleDML(any(DMLObject.class));
            verify(log, times(1)).warn(anyString(), anyString());
        }
    }

    @Nested
    class HandleDDLTest {
        DDLObject ddlObject;
        @BeforeEach
        void init() {
            ddlObject = new DDLObject();
            ddlObject.setTableVersion(1L);
            ddlObject.setTable("table");
            doNothing().when(log).debug(anyString(), anyString(), anyLong(), anyLong(), anyString());
            when(ddlEventParser.withConsumer(consumer, offset)).thenReturn(ddlEventParser);
            when(ddlEventParser.analyse(ddlObject, null, log)).thenReturn(new ArrayList<>());
            doCallRealMethod().when(manager).handleDDL(ddlObject);
        }
        @Test
        void testNormal() {
            try(MockedStatic<TapEventManager.TiOffset> to = mockStatic(TapEventManager.TiOffset.class)) {
                to.when(() -> TapEventManager.TiOffset.getVersion(offset, "table")).thenReturn(null);
                to.when(() -> TapEventManager.TiOffset.updateVersion(offset, "table", 1L)).thenAnswer(a->null);
                Assertions.assertDoesNotThrow(() -> manager.handleDDL(ddlObject));
                verify(log, times(0)).debug(anyString(), anyString(), anyLong(), anyLong(), anyString());
                verify(ddlEventParser, times(1)).withConsumer(consumer, offset);
                verify(ddlEventParser, times(1)).analyse(ddlObject, null, log);
            }
        }
        @Test
        void test1() {
            try(MockedStatic<TapEventManager.TiOffset> to = mockStatic(TapEventManager.TiOffset.class)) {
                to.when(() -> TapEventManager.TiOffset.getVersion(offset, "table")).thenReturn(1L);
                to.when(() -> TapEventManager.TiOffset.updateVersion(offset, "table", 1L)).thenAnswer(a->null);
                Assertions.assertDoesNotThrow(() -> manager.handleDDL(ddlObject));
                verify(log, times(1)).debug(anyString(), anyString(), anyLong(), anyLong(), anyString());
                verify(ddlEventParser, times(0)).withConsumer(consumer, offset);
                verify(ddlEventParser, times(0)).analyse(ddlObject, null, log);
            }
        }
        @Test
        void test2() {
            try(MockedStatic<TapEventManager.TiOffset> to = mockStatic(TapEventManager.TiOffset.class)) {
                to.when(() -> TapEventManager.TiOffset.getVersion(offset, "table")).thenReturn(-2L);
                to.when(() -> TapEventManager.TiOffset.updateVersion(offset, "table", 1L)).thenAnswer(a->null);
                Assertions.assertDoesNotThrow(() -> manager.handleDDL(ddlObject));
                verify(log, times(0)).debug(anyString(), anyString(), anyLong(), anyLong(), anyString());
                verify(ddlEventParser, times(1)).withConsumer(consumer, offset);
                verify(ddlEventParser, times(1)).analyse(ddlObject, null, log);
            }
        }

    }

    @Nested
    class HandleDMLTest {
        DMLObject dmlObject;
        @BeforeEach
        void init() {
            dmlObject = new DMLObject();
            dmlObject.setEs(1L);
            dmlObject.setTable("table");
            dmlObject.setTableVersion(1L);
            doNothing().when(log).debug(anyString(), anyString(), anyLong(), anyLong(), anyString());
            doNothing().when(consumer).accept(anyList(), anyMap());
            doCallRealMethod().when(manager).handleDML(dmlObject);
        }
        @Test
        void testNormal() {
            when(dmlEventParser.analyse(dmlObject, null, log)).thenReturn(new ArrayList<>());
            try(MockedStatic<Activity> a = mockStatic(Activity.class);
                MockedStatic<TapEventManager.TiOffset> to = mockStatic(TapEventManager.TiOffset.class)) {
                a.when(() -> Activity.getTOSTime(1L)).thenReturn(1L);
                to.when(() -> TapEventManager.TiOffset.computeIfAbsent("table", offset, 1L, 1L)).thenReturn(new HashMap<>());
                to.when(() -> TapEventManager.TiOffset.getEs(anyMap())).thenReturn(null);
                Assertions.assertDoesNotThrow(() -> manager.handleDML(dmlObject));
                verify(log, times(0)).debug(anyString(), anyString(), anyLong(), anyLong(), anyString());
                verify(dmlEventParser, times(1)).analyse(dmlObject, null, log);
                verify(consumer, times(0)).accept(anyList(), anyMap());
            }
        }
        @Test
        void testNormal1() {
            ArrayList<TapEvent> objects = new ArrayList<>();
            objects.add(new TapInsertRecordEvent());
            when(dmlEventParser.analyse(dmlObject, null, log)).thenReturn(objects);
            try(MockedStatic<Activity> a = mockStatic(Activity.class);
                MockedStatic<TapEventManager.TiOffset> to = mockStatic(TapEventManager.TiOffset.class)) {
                a.when(() -> Activity.getTOSTime(1L)).thenReturn(1L);
                to.when(() -> TapEventManager.TiOffset.computeIfAbsent("table", offset, 1L, 1L)).thenReturn(new HashMap<>());
                to.when(() -> TapEventManager.TiOffset.getEs(anyMap())).thenReturn(null);
                Assertions.assertDoesNotThrow(() -> manager.handleDML(dmlObject));
                verify(log, times(0)).debug(anyString(), anyString(), anyLong(), anyLong(), anyString());
                verify(dmlEventParser, times(1)).analyse(dmlObject, null, log);
                verify(consumer, times(1)).accept(anyList(), anyMap());
            }
        }
        @Test
        void test1() {
            try(MockedStatic<Activity> a = mockStatic(Activity.class);
                MockedStatic<TapEventManager.TiOffset> to = mockStatic(TapEventManager.TiOffset.class)) {
                a.when(() -> Activity.getTOSTime(1L)).thenReturn(1L);
                to.when(() -> TapEventManager.TiOffset.computeIfAbsent("table", offset, 1L, 1L)).thenReturn(new HashMap<>());
                to.when(() -> TapEventManager.TiOffset.getEs(anyMap())).thenReturn(2L);
                Assertions.assertDoesNotThrow(() -> manager.handleDML(dmlObject));
                verify(log, times(1)).warn(anyString(), anyString(), anyLong(), anyLong(), anyString());
                verify(dmlEventParser, times(0)).analyse(dmlObject, null, log);
                verify(consumer, times(0)).accept(anyList(), anyMap());
            }
        }
        @Test
        void test2() {
            try(MockedStatic<Activity> a = mockStatic(Activity.class);
                MockedStatic<TapEventManager.TiOffset> to = mockStatic(TapEventManager.TiOffset.class)) {
                a.when(() -> Activity.getTOSTime(1L)).thenReturn(1L);
                to.when(() -> TapEventManager.TiOffset.computeIfAbsent("table", offset, 1L, 1L)).thenReturn(new HashMap<>());
                to.when(() -> TapEventManager.TiOffset.getEs(anyMap())).thenReturn(-1L);
                Assertions.assertDoesNotThrow(() -> manager.handleDML(dmlObject));
                verify(log, times(0)).debug(anyString(), anyString(), anyLong(), anyLong(), anyString());
                verify(dmlEventParser, times(1)).analyse(dmlObject, null, log);
                verify(consumer, times(0)).accept(anyList(), anyMap());
            }
        }
    }

    @Nested
    class TiOffsetTest {
        @Test
        void testComputeIfAbsent() {
            Map<String, Long> table = TapEventManager.TiOffset.computeIfAbsent("table", new HashMap<>(), 1L, 1L);
            Assertions.assertNotNull(table);
            Assertions.assertNotNull(table.get("offset"));
            Assertions.assertNotNull(table.get("tableVersion"));
        }
        @Test
        void testGetEs() {
            Assertions.assertNull(TapEventManager.TiOffset.getEs(new HashMap<>()));
        }
        @Test
        void testGetVersion() {
            Assertions.assertNull(TapEventManager.TiOffset.getVersion(new HashMap<>(), "table"));
        }
        @Test
        void testGetMinOffset() {
            Assertions.assertNull(TapEventManager.TiOffset.getMinOffset(null));
            Assertions.assertNull(TapEventManager.TiOffset.getMinOffset("xxx"));
            Assertions.assertEquals(1L, TapEventManager.TiOffset.getMinOffset(1L));

            Map<String, Object> of = new HashMap<>();
            of.put("", new ArrayList<>());
            Assertions.assertNull(TapEventManager.TiOffset.getMinOffset(of));

            Map<String, Object> of1 = new HashMap<>();
            of1.put("t1", new HashMap<>());

            Map<String, Long> p = new HashMap<>();
            p.put("offset", 1L);
            of1.put("t2", p);

            Map<String, Long> p1 = new HashMap<>();
            p.put("offset", 6L);
            of1.put("t3", p);

            Assertions.assertNotNull(TapEventManager.TiOffset.getMinOffset(of1));
        }
        @Test
        void testUpdateVersion() {
            Map<String, Map<String, Long>> objectObjectHashMap = new HashMap<>();
            TapEventManager.TiOffset.updateVersion(objectObjectHashMap, "table", 1L);
            Assertions.assertNotNull(objectObjectHashMap.get("table"));
        }
    }
}