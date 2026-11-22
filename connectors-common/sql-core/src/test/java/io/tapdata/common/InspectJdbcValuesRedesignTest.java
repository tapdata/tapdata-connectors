package io.tapdata.common;

import org.junit.jupiter.api.Test;
import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.AdditionalAnswers.delegatesTo;

class InspectJdbcValuesRedesignTest {
    @Test void lobsXmlAndNestedArraysAreDetachedAndFreed() throws Exception {
        Clob clob = mock(Clob.class); Blob blob = mock(Blob.class); SQLXML xml = mock(SQLXML.class);
        Reader reader = mock(Reader.class, delegatesTo(new StringReader("文本\ncontent")));
        InputStream stream = mock(InputStream.class, delegatesTo(new ByteArrayInputStream(new byte[]{0, -1, 42})));
        when(clob.getCharacterStream()).thenReturn(reader); when(blob.getBinaryStream()).thenReturn(stream); when(xml.getString()).thenReturn("<v/>");
        java.sql.Array array = mock(java.sql.Array.class);
        when(array.getArray()).thenReturn(new Object[]{clob, blob, xml, new int[]{1, 2}, null});
        List<?> detached = (List<?>) InspectJdbcValues.detach(array);
        assertEquals("文本\ncontent", detached.get(0)); assertArrayEquals(new byte[]{0, -1, 42}, (byte[]) detached.get(1));
        assertEquals("<v/>", detached.get(2)); assertEquals(Arrays.asList(1, 2), detached.get(3)); assertNull(detached.get(4));
        verify(reader).close(); verify(stream).close(); verify(clob).free(); verify(blob).free(); verify(xml).free(); verify(array).free();
    }

    @Test void failingOrInterruptedLobReadsStillReleaseResources() throws Exception {
        Blob blob = mock(Blob.class); InputStream stream = mock(InputStream.class);
        when(blob.getBinaryStream()).thenReturn(stream); when(stream.read(any(byte[].class))).thenThrow(new IOException("read failed"));
        assertThrows(SQLException.class, () -> InspectJdbcValues.detach(blob)); verify(stream).close(); verify(blob).free();
        Clob clob = mock(Clob.class); when(clob.getCharacterStream()).thenReturn(new StringReader("text"));
        Thread.currentThread().interrupt();
        try { assertThrows(SQLException.class, () -> InspectJdbcValues.detach(clob)); assertTrue(Thread.currentThread().isInterrupted()); }
        finally { Thread.interrupted(); }
        verify(clob).free();
        java.sql.Array array = mock(java.sql.Array.class); when(array.getArray()).thenThrow(new SQLException("array failed"));
        assertThrows(SQLException.class, () -> InspectJdbcValues.detach(array)); verify(array).free();
    }

    @Test void emptyLargeAndNationalLobsAndByteSqlArraysKeepTheirContent() throws Exception {
        String text = String.join("", Collections.nCopies(10000, "字"));
        assertEquals(text, InspectJdbcValues.detach(new javax.sql.rowset.serial.SerialClob(text.toCharArray())));
        assertArrayEquals(new byte[0], (byte[]) InspectJdbcValues.detach(new javax.sql.rowset.serial.SerialBlob(new byte[0])));
        NClob national = mock(NClob.class); when(national.getCharacterStream()).thenReturn(new StringReader(""));
        assertEquals("", InspectJdbcValues.detach(national)); verify(national).free();
        java.sql.Array array = mock(java.sql.Array.class); when(array.getArray()).thenReturn(new byte[]{1,2});
        assertEquals(Arrays.asList((byte)1,(byte)2), InspectJdbcValues.detach(array)); verify(array).free();
    }

    @Test void postgresObjectsUseContentAndJsonNumbersKeepPrecision() throws Exception {
        // Reflection keeps the generic SQL module independent of the PG driver, including its tests.
        Object json = pg("jsonb", "{\"n\":12345678901234567890.123456789,\"items\":[1,null,true],\"s\":\"text\"}");
        Map<?, ?> content = (Map<?, ?>) InspectJdbcValues.detach(json);
        assertEquals(new BigDecimal("12345678901234567890.123456789"), content.get("n"));
        assertEquals(Arrays.asList(java.math.BigInteger.ONE, null, true), content.get("items"));
        assertEquals("(1,2)", InspectJdbcValues.detach(pg("point", "(1,2)")));
        assertNull(InspectJdbcValues.detach(pg("jsonb", null)));
        assertThrows(SQLException.class, () -> InspectJdbcValues.detach(pg("jsonb", "broken")));
        UUID id = UUID.randomUUID(); assertEquals(id.toString(), InspectJdbcValues.detach(id));
    }

    private Object pg(String type, String content) throws Exception {
        Class<?> pg = Class.forName("org.postgresql.util.PGobject"); Object value = pg.getConstructor().newInstance();
        pg.getMethod("setType", String.class).invoke(value, type); pg.getMethod("setValue", String.class).invoke(value, content); return value;
    }

    @Test void executeDetachesBeforeResultSetClosesAndLegacyKeepsItsReader() throws Throwable {
        for (boolean inspect : new boolean[]{true, false}) {
            Connection connection = mock(Connection.class); Statement statement = mock(Statement.class); ResultSet rows = mock(ResultSet.class);
            ResultSetMetaData metadata = mock(ResultSetMetaData.class); Clob clob = mock(Clob.class);
            when(connection.createStatement()).thenReturn(statement); when(connection.getAutoCommit()).thenReturn(true);
            when(statement.execute("select v")).thenReturn(true); when(statement.getResultSet()).thenReturn(rows); when(statement.getUpdateCount()).thenReturn(-1);
            when(rows.next()).thenReturn(true, false); when(rows.getMetaData()).thenReturn(metadata);
            when(metadata.getColumnCount()).thenReturn(1); when(metadata.getColumnLabel(1)).thenReturn("v"); when(metadata.getColumnTypeName(1)).thenReturn("CLOB");
            when(rows.getObject(1)).thenReturn(clob);
            when(clob.getCharacterStream()).thenAnswer(call -> { verify(rows, never()).close(); return new StringReader("actual contents"); });
            List<Object> output = new ArrayList<>(); Map<String, Object> params = new HashMap<>();
            if (inspect) params.put("inspectColumns", (Consumer<Object>) ignored -> {});
            try (InspectQueryScope scope = InspectQueryScope.open(params)) {
                JdbcQueryExecutor.execute("select v", () -> connection, output::add, () -> true, 10, 0, null,
                        rs -> { assertFalse(inspect, "Inspection must use its content reader"); return () -> Collections.singletonMap("v", "legacy conversion"); });
            }
            assertEquals(inspect ? "actual contents" : "legacy conversion", ((Map<?, ?>) ((List<?>) output.get(0)).get(0)).get("v"));
            verify(rows).close(); verify(connection).close(); if (inspect) verify(clob).free(); else verify(clob, never()).free();
        }
    }

    @Test void jdbcMetadataDistinguishesZonedAndWallClockTimestamps() throws Exception {
        ResultSet rows=mock(ResultSet.class);ResultSetMetaData meta=mock(ResultSetMetaData.class);when(rows.getMetaData()).thenReturn(meta);
        java.time.Instant instant=java.time.Instant.parse("2026-11-19T00:00:00.123456Z");
        Timestamp timestamp=Timestamp.from(instant);when(rows.getObject(1)).thenReturn(timestamp);when(rows.getObject(2)).thenReturn(timestamp);
        when(rows.getObject(3)).thenReturn(timestamp);when(meta.getColumnTypeName(1)).thenReturn("timestamptz");
        when(meta.getColumnTypeName(2)).thenReturn("timestamp");when(meta.getColumnType(3)).thenReturn(Types.TIMESTAMP_WITH_TIMEZONE);
        Map<String,Object> values=InspectJdbcValues.read(rows,Arrays.asList("zoned","local","jdbcZoned"));
        assertEquals(instant,values.get("zoned"));assertEquals(timestamp.toLocalDateTime(),values.get("local"));assertEquals(instant,values.get("jdbcZoned"));
    }

}
