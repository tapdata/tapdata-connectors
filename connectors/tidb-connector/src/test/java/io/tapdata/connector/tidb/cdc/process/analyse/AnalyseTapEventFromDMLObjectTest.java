package io.tapdata.connector.tidb.cdc.process.analyse;


import io.tapdata.connector.tidb.cdc.process.ddl.convert.Convert;
import io.tapdata.connector.tidb.cdc.process.dml.entity.DMLObject;
import io.tapdata.connector.tidb.cdc.process.dml.entity.DMLType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AnalyseTapEventFromDMLObjectTest {
    AnalyseTapEventFromDMLObject dml;
    DefaultConvert DEFAULT_CONVERT;
    @BeforeEach
    void test() {
        DEFAULT_CONVERT = new DefaultConvert();
        dml = mock(AnalyseTapEventFromDMLObject.class);
        //ReflectionTestUtils.setField(dml, "DEFAULT_CONVERT", DEFAULT_CONVERT);
    }

    @Nested
    class AnalyseTest {
        DMLObject dmlObject;
        AnalyseColumnFilter<DMLObject> filter;
        Log log;
        @BeforeEach
        void init() {
            dmlObject = new DMLObject();
            filter = null;
            log = mock(Log.class);
            when(dml.analyse(dmlObject, filter, log)).thenCallRealMethod();
            when(dml.getInsertEvent(dmlObject, filter)).thenReturn(new ArrayList<>());
            when(dml.getDeleteEvent(dmlObject, filter)).thenReturn(new ArrayList<>());
            when(dml.getUpdateEvent(dmlObject, filter)).thenReturn(new ArrayList<>());
            doNothing().when(log).warn(anyString(), anyString(), anyString());
        }

        @Test
        void testInsert() {
            dmlObject.setType(DMLType.INSERT.name());
            List<TapEvent> analyse = dml.analyse(dmlObject, filter, log);
            Assertions.assertNotNull(analyse);
            verify(dml, times(1)).getInsertEvent(dmlObject, filter);
            verify(dml, times(0)).getUpdateEvent(dmlObject, filter);
            verify(dml, times(0)).getDeleteEvent(dmlObject, filter);
            verify(log, times(0)).warn(anyString(), anyString(), anyString());
        }

        @Test
        void testUpdate() {
            dmlObject.setType(DMLType.UPDATE.name());
            List<TapEvent> analyse = dml.analyse(dmlObject, filter, log);
            Assertions.assertNotNull(analyse);
            verify(dml, times(0)).getInsertEvent(dmlObject, filter);
            verify(dml, times(1)).getUpdateEvent(dmlObject, filter);
            verify(dml, times(0)).getDeleteEvent(dmlObject, filter);
            verify(log, times(0)).warn(anyString(), anyString(), anyString());
        }

        @Test
        void testDelete() {
            dmlObject.setType(DMLType.DELETE.name());
            List<TapEvent> analyse = dml.analyse(dmlObject, filter, log);
            Assertions.assertNotNull(analyse);
            verify(dml, times(0)).getInsertEvent(dmlObject, filter);
            verify(dml, times(0)).getUpdateEvent(dmlObject, filter);
            verify(dml, times(1)).getDeleteEvent(dmlObject, filter);
            verify(log, times(0)).warn(anyString(), anyString(), anyString());
        }

        @Test
        void testUnSupport() {
            dmlObject.setType(DMLType.UN_KNOW.name());
            List<TapEvent> analyse = dml.analyse(dmlObject, filter, log);
            Assertions.assertNotNull(analyse);
            verify(dml, times(0)).getInsertEvent(dmlObject, filter);
            verify(dml, times(0)).getUpdateEvent(dmlObject, filter);
            verify(dml, times(0)).getDeleteEvent(dmlObject, filter);
            verify(log, times(1)).warn(anyString(), anyString(), anyString());
        }
    }

    @Nested
    class GetInsertEventTest {
        DMLObject dmlObject;
        AnalyseColumnFilter<DMLObject> filter;
        @BeforeEach
        void init() {
            dmlObject = new DMLObject();
            dmlObject.setTableColumnInfo(new HashMap<>());
        }
        @Test
        void testNormal() {
            List<Map<String, Object>> data = new ArrayList<>();
            data.add(new HashMap<>());
            dmlObject.setData(data);
            Map<String, Object> afterData = new HashMap<>();
            when(dml.parseData(anyMap(), anyMap())).thenReturn(afterData);
            when(dml.getInsertEvent(dmlObject, null)).thenCallRealMethod();
            List<TapEvent> insertEvent = dml.getInsertEvent(dmlObject, null);
            Assertions.assertNotNull(insertEvent);
            Assertions.assertEquals(1, insertEvent.size());
            verify(dml, times(1)).parseData(anyMap(), anyMap());
        }
        @Test
        void testDataIsEmpty() {
            List<Map<String, Object>> data = new ArrayList<>();
            dmlObject.setData(data);
            Map<String, Object> afterData = new HashMap<>();
            when(dml.parseData(anyMap(), anyMap())).thenReturn(afterData);
            when(dml.getInsertEvent(dmlObject, null)).thenCallRealMethod();
            List<TapEvent> insertEvent = dml.getInsertEvent(dmlObject, null);
            Assertions.assertNotNull(insertEvent);
            Assertions.assertEquals(0, insertEvent.size());
            verify(dml, times(0)).parseData(anyMap(), anyMap());
        }
        @Test
        void testFilterNotNull() {
            Map<String, Object> afterData = new HashMap<>();
            when(dml.parseData(anyMap(), anyMap())).thenReturn(afterData);
            filter = mock(AnalyseColumnFilter.class);
            when(filter.filter(null, afterData, dmlObject)).thenReturn(true);
            List<Map<String, Object>> data = new ArrayList<>();
            data.add(new HashMap<>());
            dmlObject.setData(data);
            when(dml.getInsertEvent(dmlObject, filter)).thenCallRealMethod();
            List<TapEvent> insertEvent = dml.getInsertEvent(dmlObject, filter);
            Assertions.assertNotNull(insertEvent);
            Assertions.assertEquals(1, insertEvent.size());
            verify(dml, times(1)).parseData(anyMap(), anyMap());
            verify(filter).filter(null, afterData, dmlObject);
        }
    }

    @Nested
    class GetUpdateEventTest {
        DMLObject dmlObject;
        AnalyseColumnFilter<DMLObject> filter;
        @BeforeEach
        void init() {
            dmlObject = new DMLObject();
            dmlObject.setTableColumnInfo(new HashMap<>());
        }

        @Test
        void testNormal() {
            Map<String, Object> afterData = new HashMap<>();
            when(dml.parseData(anyMap(), anyMap())).thenReturn(afterData);
            Map<String, Object> map = new HashMap<>();
            List<Map<String, Object>> before = new ArrayList<>();
            List<Map<String, Object>> after = new ArrayList<>();
            before.add(map);
            after.add(map);
            dmlObject.setOld(before);
            dmlObject.setData(after);
            when(dml.getUpdateEvent(dmlObject, null)).thenCallRealMethod();
            List<TapEvent> updateEvent = dml.getUpdateEvent(dmlObject, null);
            Assertions.assertNotNull(updateEvent);
            Assertions.assertEquals(1, updateEvent.size());
        }

        @Test
        void testDataIsEmpty() {
            Map<String, Object> afterData = new HashMap<>();
            when(dml.parseData(anyMap(), anyMap())).thenReturn(afterData);
            Map<String, Object> map = new HashMap<>();
            List<Map<String, Object>> before = new ArrayList<>();
            List<Map<String, Object>> after = new ArrayList<>();
            before.add(map);
            after.add(map);
            dmlObject.setOld(before);
            when(dml.getUpdateEvent(dmlObject, null)).thenCallRealMethod();
            List<TapEvent> updateEvent = dml.getUpdateEvent(dmlObject, null);
            Assertions.assertNotNull(updateEvent);
            Assertions.assertEquals(0, updateEvent.size());
        }
        @Test
        void testNotBefore() {
            Map<String, Object> afterData = new HashMap<>();
            when(dml.parseData(anyMap(), anyMap())).thenReturn(afterData);
            Map<String, Object> map = new HashMap<>();
            List<Map<String, Object>> before = new ArrayList<>();
            List<Map<String, Object>> after = new ArrayList<>();
            after.add(map);
            dmlObject.setOld(before);
            dmlObject.setData(after);
            when(dml.getUpdateEvent(dmlObject, null)).thenCallRealMethod();
            List<TapEvent> updateEvent = dml.getUpdateEvent(dmlObject, null);
            Assertions.assertNotNull(updateEvent);
            Assertions.assertEquals(1, updateEvent.size());
        }
        @Test
        void testFilterNotNull() {
            Map<String, Object> map = new HashMap<>();
            Map<String, Object> afterData = new HashMap<>();
            filter = mock(AnalyseColumnFilter.class);
            when(filter.filter(afterData, afterData, dmlObject)).thenReturn(true);
            when(dml.parseData(anyMap(), anyMap())).thenReturn(afterData);
            List<Map<String, Object>> before = new ArrayList<>();
            List<Map<String, Object>> after = new ArrayList<>();
            after.add(map);
            before.add(map);
            dmlObject.setOld(before);
            dmlObject.setData(after);
            when(dml.getUpdateEvent(dmlObject, filter)).thenCallRealMethod();
            List<TapEvent> updateEvent = dml.getUpdateEvent(dmlObject, filter);
            Assertions.assertNotNull(updateEvent);
            Assertions.assertEquals(1, updateEvent.size());
        }
    }

    @Nested
    class GetDeleteEventTest {
        DMLObject dmlObject;
        AnalyseColumnFilter<DMLObject> filter;
        @BeforeEach
        void init() {
            dmlObject = new DMLObject();
            dmlObject.setTableColumnInfo(new HashMap<>());
        }
        @Test
        void testNormal() {
            List<Map<String, Object>> data = new ArrayList<>();
            data.add(new HashMap<>());
            dmlObject.setData(data);
            Map<String, Object> afterData = new HashMap<>();
            when(dml.parseData(anyMap(), anyMap())).thenReturn(afterData);
            when(dml.getDeleteEvent(dmlObject, null)).thenCallRealMethod();
            List<TapEvent> deleteEvent = dml.getDeleteEvent(dmlObject, null);
            Assertions.assertNotNull(deleteEvent);
            Assertions.assertEquals(1, deleteEvent.size());
            verify(dml, times(1)).parseData(anyMap(), anyMap());
        }
        @Test
        void testDataIsEmpty() {
            List<Map<String, Object>> data = new ArrayList<>();
            dmlObject.setData(data);
            Map<String, Object> afterData = new HashMap<>();
            when(dml.parseData(anyMap(), anyMap())).thenReturn(afterData);
            when(dml.getDeleteEvent(dmlObject, null)).thenCallRealMethod();
            List<TapEvent> deleteEvent = dml.getDeleteEvent(dmlObject, null);
            Assertions.assertNotNull(deleteEvent);
            Assertions.assertEquals(0, deleteEvent.size());
            verify(dml, times(0)).parseData(anyMap(), anyMap());
        }
        @Test
        void testFilterNotNull() {
            Map<String, Object> afterData = new HashMap<>();
            when(dml.parseData(anyMap(), anyMap())).thenReturn(afterData);
            filter = mock(AnalyseColumnFilter.class);
            when(filter.filter(null, afterData, dmlObject)).thenReturn(true);
            List<Map<String, Object>> data = new ArrayList<>();
            data.add(new HashMap<>());
            dmlObject.setData(data);
            when(dml.getDeleteEvent(dmlObject, filter)).thenCallRealMethod();
            List<TapEvent> deleteEvent = dml.getDeleteEvent(dmlObject, filter);
            Assertions.assertNotNull(deleteEvent);
            Assertions.assertEquals(1, deleteEvent.size());
            verify(dml, times(1)).parseData(anyMap(), anyMap());
            verify(filter).filter(null, afterData, dmlObject);
        }
    }

    @Nested
    class ParseDataTest {
        Map<String, Object> data;
        Map<String, Convert> mysqlType;
        @BeforeEach
        void init() {
            data = new HashMap<>();
            data.put("key", 100);
            mysqlType = new HashMap<>();
            mysqlType.put("key", new io.tapdata.connector.tidb.cdc.process.ddl.convert.DefaultConvert());
        }
        @Test
        void testNormal() {
            when(dml.parseData(data, mysqlType)).thenCallRealMethod();
            Map<String, Object> map = dml.parseData(data, mysqlType);
            Assertions.assertNotNull(map);
            Assertions.assertNotNull(map.get("key"));
        }
        @Test
        void testDataIsEmpty() {
            data.clear();
            when(dml.parseData(data, mysqlType)).thenCallRealMethod();
            Map<String, Object> map = dml.parseData(data, mysqlType);
            Assertions.assertNotNull(map);
            Assertions.assertNull(map.get("key"));
        }
        @Test
        void testMySqlTypeIsEmpty() {
            mysqlType.clear();
            when(dml.parseData(data, mysqlType)).thenCallRealMethod();
            Map<String, Object> map = dml.parseData(data, mysqlType);
            Assertions.assertNotNull(map);
            Assertions.assertNotNull(map.get("key"));
        }
        @Test
        void testValueIsNull() {
            data.put("key", null);
            when(dml.parseData(data, mysqlType)).thenCallRealMethod();
            Map<String, Object> map = dml.parseData(data, mysqlType);
            Assertions.assertNotNull(map);
            Assertions.assertNull(map.get("key"));
        }
        @Test
        void testMysqlTypeIsNull() {
            mysqlType.put("key", null);
            when(dml.parseData(data, mysqlType)).thenCallRealMethod();
            Map<String, Object> map = dml.parseData(data, mysqlType);
            Assertions.assertNotNull(map);
            Assertions.assertNotNull(map.get("key"));
        }
    }
}