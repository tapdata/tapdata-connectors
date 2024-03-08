package io.tapdata.connector.gauss.cdc.logic.event.dml;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Map;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class CollectEntityTest {
    CollectEntity entity;
    @BeforeEach
    void init() {
        entity = mock(CollectEntity.class);
        entity.after = mock(Map.class);
        entity.before = mock(Map.class);
        entity.fieldType = mock(Map.class);
        entity.schema = "schema";
        entity.table = "table";
    }

    @Test
    void testInstance() {
        try (MockedStatic<CollectEntity> gr = mockStatic(CollectEntity.class)){
            gr.when(CollectEntity::instance).thenCallRealMethod();
            Assertions.assertNotNull(CollectEntity.instance());
        }
    }

    @Test
    void testGetBefore() {
        when(entity.getBefore()).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> entity.getBefore());
    }

    @Test
    void testWithBefore() {
        when(entity.withBefore(anyMap())).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> entity.withBefore(mock(Map.class)));
    }

    @Test
    void testGetAfter() {
        when(entity.getAfter()).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> entity.getAfter());
    }

    @Test
    void testWithAfter() {
        when(entity.withAfter(anyMap())).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> entity.withAfter(mock(Map.class)));
    }

    @Test
    void testGetFieldType() {
        when(entity.getFieldType()).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> entity.getFieldType());
    }

    @Test
    void testWithFieldType() {
        when(entity.withFieldType(anyMap())).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> entity.withFieldType(mock(Map.class)));
    }

    @Test
    void testGetSchema() {
        when(entity.getSchema()).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> entity.getSchema());
    }

    @Test
    void testGetTable() {
        when(entity.getTable()).thenCallRealMethod();
        Assertions.assertDoesNotThrow(() -> entity.getTable());
    }

    @Test
    void testSetSchema() {
        doCallRealMethod().when(entity).setSchema("schema");
        Assertions.assertDoesNotThrow(() -> entity.setSchema("schema"));
    }

    @Test
    void testSetTable() {
        doCallRealMethod().when(entity).setTable("table");
        Assertions.assertDoesNotThrow(() -> entity.setTable("table"));
    }

    @Test
    void testWithSchema() {
        when(entity.withSchema("")).thenCallRealMethod();
        entity.withSchema("");
    }


    @Test
    void testWithTable() {
        when(entity.withTable("")).thenCallRealMethod();
        entity.withTable("");
    }
}
