package io.tapdata.kit;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

/**
 * tools for ResultSet
 *
 * @author Jarad
 * @date 2022/5/29
 */
public class DbKit {

    /**
     * get all data from ResultSet starting with current row
     *
     * @param resultSet ResultSet
     * @return list<Map>
     */
    public static List<DataMap> getDataFromResultSet(ResultSet resultSet) throws SQLException {
        List<DataMap> list = TapSimplify.list();
        if (EmptyKit.isNotNull(resultSet)) {
            List<String> columnNames = getColumnsFromResultSet(resultSet);
            //cannot replace with while resultSet.next()
            while (resultSet.next()) {
                list.add(getRowFromResultSet(resultSet, columnNames));
            }
        }
        return list;
    }

    /**
     * get current row
     *
     * @param resultSet   ResultSet
     * @param columnNames column names of ResultSet
     * @return Map
     */
    public static DataMap getRowFromResultSet(ResultSet resultSet, Collection<String> columnNames) throws SQLException {
        DataMap map = DataMap.create();
        if (EmptyKit.isNotNull(resultSet) && resultSet.getRow() > 0) {
            String errorCol = null;
            int index = 1;
            for (String col : columnNames) {
                try {
                    map.put(col, resultSet.getObject(index++));
                } catch (Exception e) {
                    errorCol = col;
                }
            }
            if (EmptyKit.isNotNull(errorCol)) {
                TapLogger.warn("JDBC ERROR", "row: {}, skip {}", toJson(map), errorCol);
            }
        }
        return map;
    }

    /**
     * get column names from ResultSet
     *
     * @param resultSet ResultSet
     * @return List<String>
     */
    public static List<String> getColumnsFromResultSet(ResultSet resultSet) throws SQLException {
        //get all column names
        List<String> columnNames = new ArrayList<>();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String[] columnNameArr = resultSetMetaData.getColumnLabel(i).split("\\.");
            String substring = columnNameArr[columnNameArr.length - 1];
            columnNames.add(substring);
        }
        return columnNames;
    }

    public static List<String> getColumnTypesFromResultSet(ResultSet resultSet) throws SQLException {
        //get all column typeNames
        List<String> columnTypeNames = new ArrayList<>();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            columnTypeNames.add(resultSetMetaData.getColumnTypeName(i));
        }
        return columnTypeNames;
    }

    public static List<Object> getDataArrayByColumnName(ResultSet resultSet, String columnName) throws SQLException {
        List<Object> list = TapSimplify.list();
        while (resultSet.next()) {
            list.add(resultSet.getObject(columnName));
        }
        return list;
    }

    public static byte[] blobToBytes(Blob blob) {
        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(blob.getBinaryStream());
            byte[] bytes = new byte[(int) blob.length()];
            int len = bytes.length;
            int offset = 0;
            int read;
            while (offset < len && (read = bis.read(bytes, offset, len - offset)) > 0) {
                offset += read;
            }
            return bytes;
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static String clobToString(Clob clob) {
        if (clob == null) {
            return null;
        }
        String re;
        try (Reader is = clob.getCharacterStream(); BufferedReader br = new BufferedReader(is)) {
            String s = br.readLine();
            StringBuilder sb = new StringBuilder();
            while (s != null) {
                sb.append(s);
                s = br.readLine();
            }
            re = sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return re;
    }

    public static boolean ignoreCreateIndex(TapIndex exists, TapIndex created) {
        if (!exists.isUnique() && created.isUnique()) {
            return false;
        }
        return exists.getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList())
                .equals(created.getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList()));
    }

    public static String buildIndexName(String table) {
        return "TAPIDX_" + table.substring(Math.max(table.length() - 10, 0)) + UUID.randomUUID().toString().replaceAll("-", "").substring(20);
    }

    public static <T> List<List<T>> splitToPieces(List<T> data, int eachPieceSize) {
        if (EmptyKit.isEmpty(data)) {
            return new ArrayList<>();
        }
        if (eachPieceSize <= 0) {
            throw new IllegalArgumentException("Param Error");
        }
        List<List<T>> result = new ArrayList<>();
        for (int index = 0; index < data.size(); index += eachPieceSize) {
            result.add(data.stream().skip(index).limit(eachPieceSize).collect(Collectors.toList()));
        }
        return result;
    }

    public static Map<String, Object> getBeforeForUpdate(Map<String, Object> after, Map<String, Object> before, Collection<String> allColumn, Collection<String> uniqueCondition) {
        //in some datasource, before of events is always empty, so before is unreliable
        Map<String, Object> lastBefore = new HashMap<>();
        if (EmptyKit.isEmpty(uniqueCondition)) {
            allColumn.forEach(v -> lastBefore.put(v, (EmptyKit.isNotEmpty(before) && before.containsKey(v)) ? before.get(v) : after.get(v)));
        } else {
            uniqueCondition.forEach(v -> lastBefore.put(v, (EmptyKit.isNotEmpty(before) && before.containsKey(v)) ? before.get(v) : after.get(v)));
        }
        return lastBefore;
    }

    public static Map<String, Object> getAfterForUpdate(Map<String, Object> after, Map<String, Object> before, Collection<String> allColumn, Collection<String> uniqueCondition) {
        Map<String, Object> lastBefore = getBeforeForUpdate(after, before, allColumn, uniqueCondition);
        if (EmptyKit.isNotEmpty(before)) {
            lastBefore.putAll(before);
        }
        Map<String, Object> lastAfter = new HashMap<>();
        for (Map.Entry<String, Object> entry : after.entrySet()) {
            if (EmptyKit.isNull(entry.getValue()) && lastBefore.containsKey(entry.getKey()) && EmptyKit.isNull(lastBefore.get(entry.getKey())) || EmptyKit.isNotNull(entry.getValue()) && entry.getValue().equals(lastBefore.get(entry.getKey()))) {
                continue;
            }
            lastAfter.put(entry.getKey(), entry.getValue());
        }
        if (EmptyKit.isEmpty(lastAfter)) {
            return after;
        }
        return lastAfter;
    }
}
