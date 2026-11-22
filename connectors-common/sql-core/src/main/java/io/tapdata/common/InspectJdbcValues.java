package io.tapdata.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.sql.*;
import java.util.*;

/** Detaches inspection values before advancing or closing the JDBC result set.
 * Only JDK values leave the connector classloader. Legacy command conversion is unchanged. */
final class InspectJdbcValues {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private InspectJdbcValues() { }

    static Map<String, Object> read(ResultSet rows, List<String> columns) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        ResultSetMetaData metadata = rows.getMetaData();
        for (int i = 0; i < columns.size(); i++) {
            Object value = rows.getObject(i + 1);
            if (value instanceof Timestamp) {
                String type = metadata.getColumnTypeName(i + 1);
                String normalized = type == null ? "" : type.toLowerCase(Locale.ROOT);
                boolean zoned = metadata.getColumnType(i + 1) == Types.TIMESTAMP_WITH_TIMEZONE
                        || normalized.equals("timestamptz") || normalized.contains("with time zone")
                        || normalized.equals("datetimeoffset");
                value = zoned ? ((Timestamp) value).toInstant() : ((Timestamp) value).toLocalDateTime();
            }
            row.put(columns.get(i), detach(value));
        }
        return row;
    }

    static Object detach(Object value) throws SQLException {
        if (value == null) return null;
        if (value instanceof UUID) return value.toString();
        if (value instanceof Clob) {
            Clob clob = (Clob) value;
            try (Reader reader = clob.getCharacterStream()) {
                StringBuilder text = new StringBuilder(); char[] buffer = new char[8192]; int count;
                while ((count = reader.read(buffer)) != -1) { checkInterrupted(); text.append(buffer, 0, count); }
                return text.toString();
            } catch (IOException e) { throw new SQLException("Cannot read query CLOB", e); }
            finally { clob.free(); }
        }
        if (value instanceof Blob) {
            Blob blob = (Blob) value;
            try (InputStream stream = blob.getBinaryStream(); ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[8192]; int count;
                while ((count = stream.read(buffer)) != -1) { checkInterrupted(); bytes.write(buffer, 0, count); }
                return bytes.toByteArray();
            } catch (IOException e) { throw new SQLException("Cannot read query BLOB", e); }
            finally { blob.free(); }
        }
        if (value instanceof java.sql.Array) {
            java.sql.Array array = (java.sql.Array) value;
            try {
                Object elements = array.getArray();
                return elements == null ? null : detachArray(elements);
            } finally { array.free(); }
        }
        if (value instanceof SQLXML) {
            SQLXML xml = (SQLXML) value;
            try { return xml.getString(); } finally { xml.free(); }
        }
        // No compile-time dependency on the PostgreSQL driver. Subclasses include geometry types.
        for (Class<?> type = value.getClass(); type != null; type = type.getSuperclass()) {
            if (!"org.postgresql.util.PGobject".equals(type.getName())) continue;
            try {
                String content = (String) type.getMethod("getValue").invoke(value);
                String sqlType = (String) type.getMethod("getType").invoke(value);
                if (content != null && ("json".equalsIgnoreCase(sqlType) || "jsonb".equalsIgnoreCase(sqlType)))
                    return JSON.readValue(content, Object.class);
                return content;
            } catch (ReflectiveOperationException | IOException e) {
                throw new SQLException("Cannot read PostgreSQL query value", e);
            }
        }
        if (value instanceof byte[]) return ((byte[]) value).clone();
        if (value.getClass().isArray()) return detachArray(value);
        if (value instanceof List) {
            List<Object> result = new ArrayList<>();
            for (Object element : (List<?>) value) result.add(detach(element));
            return result;
        }
        if (value instanceof Map) {
            Map<Object, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) result.put(entry.getKey(), detach(entry.getValue()));
            return result;
        }
        return value;
    }

    private static List<Object> detachArray(Object value) throws SQLException {
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < java.lang.reflect.Array.getLength(value); i++) {
            checkInterrupted(); result.add(detach(java.lang.reflect.Array.get(value, i)));
        }
        return result;
    }

    private static void checkInterrupted() throws SQLException {
        if (Thread.currentThread().isInterrupted()) throw new SQLException("Inspection value read cancelled");
    }
}
