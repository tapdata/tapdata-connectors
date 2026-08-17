package io.tapdata.connector.postgres.cdc.physical;

import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class RelationCatalogTest {

    @Test
    public void testLookupAndCache() throws Exception {
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        Log log = mock(Log.class);

        ResultSet rel = mock(ResultSet.class);
        when(rel.next()).thenReturn(true);
        when(rel.getLong("oid")).thenReturn(16384L);
        when(rel.getString("nspname")).thenReturn("public");
        when(rel.getString("relname")).thenReturn("t");

        ResultSet cols = mock(ResultSet.class);
        when(cols.next()).thenReturn(true, true, false);
        when(cols.getString("attname")).thenReturn("id", "name");
        when(cols.getInt("attnum")).thenReturn(1, 2);
        when(cols.getLong("atttypid")).thenReturn(23L, 25L);
        when(cols.getInt("attlen")).thenReturn(4, -1);
        when(cols.getString("attalign")).thenReturn("i", "i");
        when(cols.getBoolean("attisdropped")).thenReturn(false, false);

        ResultSet keys = mock(ResultSet.class);
        when(keys.next()).thenReturn(true, false);
        when(keys.getString("attname")).thenReturn("id");

        doAnswer(inv -> {
            String sql = inv.getArgument(0);
            ResultSetConsumer c = inv.getArgument(1);
            if (sql.contains("pg_class")) {
                c.accept(rel);
            } else if (sql.contains("pg_attribute") && sql.contains("ORDER BY")) {
                c.accept(cols);
            } else {
                c.accept(keys);
            }
            return null;
        }).when(ctx).query(anyString(), any(ResultSetConsumer.class));

        RelationCatalog catalog = new RelationCatalog(ctx, log);
        RelationInfo info = catalog.lookup(16384L);
        assertNotNull(info);
        assertEquals("public", info.schema);
        assertEquals("t", info.table);
        assertEquals(2, info.columns.size());
        assertEquals("id", info.columns.get(0).name);
        assertEquals(23L, info.columns.get(0).typeOid);
        assertEquals(-1, info.columns.get(1).typLen);
        assertEquals(1, info.keyColumns.size());
        assertEquals("id", info.keyColumns.get(0));

        // second lookup is served from cache -> no extra queries (3 total)
        catalog.lookup(16384L);
        verify(ctx, times(3)).query(anyString(), any(ResultSetConsumer.class));
    }

    @Test
    public void testNegativeCache() throws Exception {
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        Log log = mock(Log.class);
        ResultSet empty = mock(ResultSet.class);
        when(empty.next()).thenReturn(false);
        doAnswer(inv -> {
            ((ResultSetConsumer) inv.getArgument(1)).accept(empty);
            return null;
        }).when(ctx).query(anyString(), any(ResultSetConsumer.class));

        RelationCatalog catalog = new RelationCatalog(ctx, log);
        assertNull(catalog.lookup(99999L));
        assertNull(catalog.lookup(99999L));
        // only the pg_class probe runs once; negative cache prevents re-query
        verify(ctx, times(1)).query(anyString(), any(ResultSetConsumer.class));
    }

    @Test
    public void testLookupLoadsEnumLabelsForEnumAndArrayColumns() throws Exception {
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        Log log = mock(Log.class);

        ResultSet rel = mock(ResultSet.class);
        when(rel.next()).thenReturn(true);
        when(rel.getLong("oid")).thenReturn(16384L);
        when(rel.getString("nspname")).thenReturn("public");
        when(rel.getString("relname")).thenReturn("cyber_draw");

        ResultSet cols = mock(ResultSet.class);
        when(cols.next()).thenReturn(true, true, false);
        when(cols.getString("attname")).thenReturn("status", "drawMode");
        when(cols.getInt("attnum")).thenReturn(1, 2);
        when(cols.getLong("atttypid")).thenReturn(50001L, 50002L);
        when(cols.getLong("enumtypid")).thenReturn(50001L, 50001L);
        when(cols.getInt("attlen")).thenReturn(4, -1);
        when(cols.getString("attalign")).thenReturn("i", "i");
        when(cols.getBoolean("attisdropped")).thenReturn(false, false);

        ResultSet enumLabels = mock(ResultSet.class);
        when(enumLabels.next()).thenReturn(true, true, false);
        when(enumLabels.getLong("oid")).thenReturn(70001L, 70002L);
        when(enumLabels.getString("enumlabel")).thenReturn("DRAFT", "APPROVED");

        ResultSet keys = mock(ResultSet.class);
        when(keys.next()).thenReturn(false);

        doAnswer(inv -> {
            String sql = inv.getArgument(0);
            ResultSetConsumer c = inv.getArgument(1);
            if (sql.contains("pg_class")) {
                c.accept(rel);
            } else if (sql.contains("pg_attribute") && sql.contains("ORDER BY")) {
                c.accept(cols);
            } else if (sql.contains("pg_enum")) {
                c.accept(enumLabels);
            } else {
                c.accept(keys);
            }
            return null;
        }).when(ctx).query(anyString(), any(ResultSetConsumer.class));

        RelationCatalog catalog = new RelationCatalog(ctx, log);
        RelationInfo info = catalog.lookup(1L);

        assertEquals(2, info.columns.size());
        assertEquals(50001L, info.columns.get(0).enumTypeOid);
        assertEquals("DRAFT", info.columns.get(0).enumLabels.get(70001L));
        assertEquals(50001L, info.columns.get(1).enumTypeOid);
        assertEquals("APPROVED", info.columns.get(1).enumLabels.get(70002L));
        verify(ctx, times(1)).query(contains("pg_enum"), any(ResultSetConsumer.class));
    }

    @Test
    public void testLookupKeepsDroppedColumnPlaceholder() throws Exception {
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        Log log = mock(Log.class);

        ResultSet rel = mock(ResultSet.class);
        when(rel.next()).thenReturn(true);
        when(rel.getLong("oid")).thenReturn(16384L);
        when(rel.getString("nspname")).thenReturn("public");
        when(rel.getString("relname")).thenReturn("cyber_draw");

        ResultSet cols = mock(ResultSet.class);
        when(cols.next()).thenReturn(true, true, true, false);
        when(cols.getString("attname")).thenReturn("type", "........pg.dropped.13........", "max_won_per_patron");
        when(cols.getInt("attnum")).thenReturn(12, 13, 14);
        when(cols.getLong("atttypid")).thenReturn(50001L, 0L, 23L);
        when(cols.getLong("enumtypid")).thenReturn(50001L, 0L, 0L);
        when(cols.getInt("attlen")).thenReturn(4, -1, 4);
        when(cols.getString("attalign")).thenReturn("i", "i", "i");
        when(cols.getBoolean("attisdropped")).thenReturn(false, true, false);

        ResultSet enumLabels = mock(ResultSet.class);
        when(enumLabels.next()).thenReturn(true, false);
        when(enumLabels.getLong("oid")).thenReturn(70001L);
        when(enumLabels.getString("enumlabel")).thenReturn("GAMING");

        ResultSet keys = mock(ResultSet.class);
        when(keys.next()).thenReturn(false);

        doAnswer(inv -> {
            String sql = inv.getArgument(0);
            ResultSetConsumer c = inv.getArgument(1);
            if (sql.contains("pg_class")) {
                c.accept(rel);
            } else if (sql.contains("pg_attribute") && sql.contains("ORDER BY")) {
                c.accept(cols);
            } else if (sql.contains("pg_enum")) {
                c.accept(enumLabels);
            } else {
                c.accept(keys);
            }
            return null;
        }).when(ctx).query(anyString(), any(ResultSetConsumer.class));

        RelationCatalog catalog = new RelationCatalog(ctx, log);
        RelationInfo info = catalog.lookup(1L);

        assertEquals(3, info.columns.size());
        assertEquals(13, info.columns.get(1).attnum);
        assertTrue(info.columns.get(1).dropped);
        assertEquals(14, info.columns.get(2).attnum);
        verify(ctx).query(contains("LEFT JOIN pg_type"), any(ResultSetConsumer.class));
    }

    @Test
    public void applyPendingChangesSkipsSystemColumnsAndReplacesExistingAttnum() {
        RelationInfo rel = new RelationInfo("public", "t",
                Arrays.asList(
                        new ColumnInfo("id", 1, PgTypeDecoder.INT4, 4, 'i', false),
                        new ColumnInfo("name", 2, PgTypeDecoder.TEXT, -1, 'i', false)),
                Collections.singletonList("id"), false);

        List<RelationCatalog.PgAttributeChange> pending = Arrays.asList(
                new RelationCatalog.PgAttributeChange("INSERT", -6, "tableoid", PgTypeDecoder.INT4, 4, 'i', false),
                new RelationCatalog.PgAttributeChange("INSERT", -1, "ctid", PgTypeDecoder.TEXT, -1, 'i', false),
                new RelationCatalog.PgAttributeChange("INSERT", 1, "id", PgTypeDecoder.INT4, 4, 'i', false),
                new RelationCatalog.PgAttributeChange("INSERT", 2, "name", PgTypeDecoder.TEXT, -1, 'i', false),
                new RelationCatalog.PgAttributeChange("INSERT", 3, "age", PgTypeDecoder.INT4, 4, 'i', false));

        RelationInfo updated = RelationCatalog.applyPendingChanges(rel, pending);

        assertEquals(3, updated.columns.size());
        assertEquals("id", updated.columns.get(0).name);
        assertEquals(1, updated.columns.get(0).attnum);
        assertEquals("name", updated.columns.get(1).name);
        assertEquals(2, updated.columns.get(1).attnum);
        assertEquals("age", updated.columns.get(2).name);
        assertEquals(3, updated.columns.get(2).attnum);
    }
}
