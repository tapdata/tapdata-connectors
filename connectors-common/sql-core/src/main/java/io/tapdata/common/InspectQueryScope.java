package io.tapdata.common;

import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

/** Optional command parameters used by inspection; no connector API changes. */
public final class InspectQueryScope implements AutoCloseable {
    private static final ThreadLocal<Map<String, Object>> CURRENT = new ThreadLocal<>();
    private final Map<String, Object> previous;
    private InspectQueryScope(Map<String, Object> params) { previous = CURRENT.get(); CURRENT.set(params); }
    public static InspectQueryScope open(Map<String, Object> params) { return new InspectQueryScope(params); }
    public static boolean active() { return CURRENT.get() != null && CURRENT.get().containsKey("inspectColumns"); }
    public static void register(Statement statement, Connection connection) {
        Map<String, Object> params = CURRENT.get();
        if (params != null && params.get("inspectRegisterCancel") instanceof Consumer)
            ((Consumer<Runnable>) params.get("inspectRegisterCancel")).accept(() -> { try { statement.cancel(); } catch (SQLException ignored) { } finally { try { connection.close(); } catch (SQLException ignored) { } } });
    }
    public static List<String> rowColumns(ResultSet resultSet) throws SQLException {
        if (!active()) return io.tapdata.kit.DbKit.getColumnsFromResultSet(resultSet);
        List<String> names = new ArrayList<>(); ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) names.add(metadata.getColumnLabel(i));
        return names;
    }
    public static void columns(ResultSet resultSet) throws SQLException {
        Map<String, Object> params = CURRENT.get();
        if (params == null || !(params.get("inspectColumns") instanceof Consumer)) return;
        ResultSetMetaData meta = resultSet.getMetaData();
        List<Map<String, String>> columns = new ArrayList<>();
        Set<String> aliases = new HashSet<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String name = meta.getColumnLabel(i);
            if (!aliases.add(name)) throw new SQLException("Duplicate query output alias: " + name);
            Map<String, String> column = new LinkedHashMap<>();
            column.put("name", name); column.put("type", meta.getColumnTypeName(i)); columns.add(column);
        }
        ((Consumer<List<Map<String, String>>>) params.get("inspectColumns")).accept(columns);
    }
    public void close() { if (previous == null) CURRENT.remove(); else CURRENT.set(previous); }
}
