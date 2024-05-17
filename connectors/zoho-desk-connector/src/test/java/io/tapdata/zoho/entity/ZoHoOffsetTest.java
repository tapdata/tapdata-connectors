package io.tapdata.zoho.entity;


import io.tapdata.zoho.service.zoho.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ZoHoOffsetTest {
    @Nested
    class CreateTest {
        @Test
        void testNormal() {
            ZoHoOffset zoHoOffset = ZoHoOffset.create(null);
            Assertions.assertNotNull(zoHoOffset);
            Assertions.assertNotNull(zoHoOffset.getTableUpdateTimeMap());
            Assertions.assertEquals(0, zoHoOffset.getTableUpdateTimeMap().size());
            zoHoOffset.setTableUpdateTimeMap(new HashMap<>());
        }
    }

    @Nested
    class MapTest {
        @Test
        void testNull() {
            Map<String, Long> zoHoOffset = ZoHoOffset.map(null);
            Assertions.assertNotNull(zoHoOffset);
            Assertions.assertEquals(0, zoHoOffset.size());
        }
        @Test
        void testNormal() {
            Map<String, Long> zoHoOffset = ZoHoOffset.map(new HashMap<>());
            Assertions.assertNotNull(zoHoOffset);
            Assertions.assertEquals(0, zoHoOffset.size());
        }
    }

    @Nested
    class MapFromSchemaTest {
        List<Schema> schemas;
        @BeforeEach
        void init() {
            schemas = new ArrayList<>();
        }

        @Test
        void testNormal() {
            Map<String, Long> stringLongMap = ZoHoOffset.mapFromSchema(schemas, 0L);
            Assertions.assertNotNull(stringLongMap);
            Assertions.assertEquals(HashMap.class.getName(), stringLongMap.getClass().getName());
            Assertions.assertEquals(0, stringLongMap.size());
        }

        @Test
        void testNull() {
            Map<String, Long> stringLongMap = ZoHoOffset.mapFromSchema(null, 0L);
            Assertions.assertNotNull(stringLongMap);
            Assertions.assertEquals(HashMap.class.getName(), stringLongMap.getClass().getName());
            Assertions.assertEquals(0, stringLongMap.size());
        }

        @Test
        void testHasItem() {
            Schema schema = mock(Schema.class);
            when(schema.schemaName()).thenReturn("name");
            schemas.add(schema);
            Map<String, Long> stringLongMap = ZoHoOffset.mapFromSchema(schemas, 0L);
            Assertions.assertNotNull(stringLongMap);
            Assertions.assertEquals(HashMap.class.getName(), stringLongMap.getClass().getName());
            Assertions.assertEquals(1, stringLongMap.size());
        }

        @Test
        void testHasNullItem() {
            Schema schema = mock(Schema.class);
            when(schema.schemaName()).thenReturn("name");
            schemas.add(schema);
            schemas.add(null);
            Map<String, Long> stringLongMap = ZoHoOffset.mapFromSchema(schemas, 0L);
            Assertions.assertNotNull(stringLongMap);
            Assertions.assertEquals(HashMap.class.getName(), stringLongMap.getClass().getName());
            Assertions.assertEquals(1, stringLongMap.size());
        }
    }

    @Nested
    class OffsetTest {
        @Test
        void testNormal() {
            ZoHoOffset zoHoOffset = ZoHoOffset.create(null);
            Object offset = zoHoOffset.offset();
            Assertions.assertNotNull(offset);
            Assertions.assertEquals(HashMap.class.getName(), offset.getClass().getName());
            Assertions.assertEquals(1, ((HashMap<String, Object>)offset).size());
            Assertions.assertNotNull(((HashMap<String, Object>)offset).get(ZoHoOffset.TABLE_UPDATE_TIME_MAP));
            Assertions.assertEquals(HashMap.class.getName(), ((HashMap<String, Object>)offset).get(ZoHoOffset.TABLE_UPDATE_TIME_MAP).getClass().getName());
        }
    }

    @Nested
    class FromTest {

        @Test
        void testNormal() {
            ZoHoOffset from = ZoHoOffset.from(null);
            Assertions.assertNotNull(from);
            Assertions.assertNotNull(from.getTableUpdateTimeMap());
            Assertions.assertEquals(0, from.getTableUpdateTimeMap().size());
        }

        @Test
        void testFromMap() {
            ZoHoOffset from = ZoHoOffset.from(new HashMap<>());
            Assertions.assertNotNull(from);
            Assertions.assertNotNull(from.getTableUpdateTimeMap());
            Assertions.assertEquals(0, from.getTableUpdateTimeMap().size());
        }

        @Test
        void testFromMap2() {
            HashMap<Object, Object> objectObjectHashMap = new HashMap<>();
            objectObjectHashMap.put(ZoHoOffset.TABLE_UPDATE_TIME_MAP, 0);
            ZoHoOffset from = ZoHoOffset.from(objectObjectHashMap);
            Assertions.assertNotNull(from);
            Assertions.assertNotNull(from.getTableUpdateTimeMap());
            Assertions.assertEquals(0, from.getTableUpdateTimeMap().size());
        }
        @Test
        void testFromMap3() {
            HashMap<Object, Object> objectObjectHashMap = new HashMap<>();
            objectObjectHashMap.put(ZoHoOffset.TABLE_UPDATE_TIME_MAP, new HashMap<String, Long>());
            ZoHoOffset from = ZoHoOffset.from(objectObjectHashMap);
            Assertions.assertNotNull(from);
            Assertions.assertNotNull(from.getTableUpdateTimeMap());
            Assertions.assertEquals(0, from.getTableUpdateTimeMap().size());
        }
    }
}