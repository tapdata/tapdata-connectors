package io.tapdata.oceanbase.cdc;

import io.tapdata.data.ob.ReadLogPayload;
import io.tapdata.data.ob.Value;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OceanbaseReaderV2DdlTest {
    @Test
    void decodesAddedColumnsWithoutAnEngineSchemaRefreshAndRemovesDroppedColumn() {
        TapTable original = new TapTable("orders").add(new TapField("id", "BIGINT"));
        @SuppressWarnings("unchecked")
        KVReadOnlyMap<TapTable> tables = mock(KVReadOnlyMap.class);
        when(tables.get("orders")).thenReturn(original);
        OceanbaseReaderV2 reader = new OceanbaseReaderV2(new OceanbaseConfig(), "test") {
            @Override protected void initSource() { }
        };
        reader.init(List.of("orders"), tables, 0L, 100, null);
        TapNewFieldEvent add = new TapNewFieldEvent();
        add.setTableId("orders");
        add.field(new TapField("c_composite_a", "VARCHAR(64)"));
        add.field(new TapField("c_composite_b", "BIGINT UNSIGNED"));
        reader.applyDdlToCachedSchema(add);
        ReadLogPayload inserted = ReadLogPayload.newBuilder().setTbname("orders").setOpValue(3)
                .addAfter(Value.newBuilder().setColumnName("id").setValueString("1"))
                .addAfter(Value.newBuilder().setColumnName("c_composite_a").setValueString("marker"))
                .addAfter(Value.newBuilder().setColumnName("c_composite_b").setValueString("42")).build();
        Map<String, Object> row = ReflectionTestUtils.invokeMethod(reader, "getRecord", inserted, false);
        assertEquals("marker", row.get("c_composite_a"));
        assertEquals(new BigDecimal("42"), row.get("c_composite_b"));
        assertEquals(1, original.getNameFieldMap().size(), "reader must not depend on mutating the shared table map");

        TapDropFieldEvent drop = new TapDropFieldEvent().fieldName("c_composite_a");
        drop.setTableId("orders");
        reader.applyDdlToCachedSchema(drop);
        ReadLogPayload afterDrop = ReadLogPayload.newBuilder().setTbname("orders").setOpValue(3)
                .addAfter(Value.newBuilder().setColumnName("id").setValueString("2"))
                .addAfter(Value.newBuilder().setColumnName("c_composite_b").setValueString("43")).build();
        Map<String, Object> next = ReflectionTestUtils.invokeMethod(reader, "getRecord", afterDrop, false);
        assertFalse(next.containsKey("c_composite_a"));
        assertEquals(new BigDecimal("43"), next.get("c_composite_b"));
        assertThrows(IllegalStateException.class, () -> ReflectionTestUtils.invokeMethod(reader, "getRecord", inserted, false),
                "dropped column metadata must not be retained");
    }
}
