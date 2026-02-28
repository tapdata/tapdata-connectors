package io.tapdata.common;

import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.functions.TapSupplier;
import io.tapdata.util.DateUtil;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class DefaultSqlExecutor implements SqlExecutor {

    private static final String PARAMS_NAME = "name";
    private static final String PARAMS_TYPE = "type";
    private static final String PARAMS_MODE = "mode";
    private static final String PARAMS_VALUE = "value";
    private static final String MODE_IN = "in";
    private static final String MODE_OUT = "out";
    private static final String MODE_IN_OUT = "in/out";
    private static final String MODE_RETURN = "return";

    private Consumer<DataMap> processDataMapConsumer;

    public DefaultSqlExecutor(Consumer<DataMap> processDataMapConsumer) {
        this.processDataMapConsumer = processDataMapConsumer;
    }

    public DefaultSqlExecutor() {
    }

    @Override
    public void execute(String sql, TapSupplier<Connection> connectionSupplier, Consumer<Object> consumer, Supplier<Boolean> aliveSupplier, int batchSize) throws Throwable {
        try (Connection connection = connectionSupplier.get();
             Statement sqlStatement = connection.createStatement()) {
            boolean hasResult = sqlStatement.execute(sql);
            if (!hasResult) {
                consumer.accept((long) sqlStatement.getUpdateCount());
            } else {
                while (aliveSupplier.get() && hasResult) {
                    try (ResultSet resultSet = sqlStatement.getResultSet()) {
                        List<Map<String, Object>> list = TapSimplify.list();
                        List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                        while (aliveSupplier.get() && resultSet.next()) {
                            DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                            processDataMap(dataMap);
                            list.add(dataMap);
                            if (list.size() == batchSize) {
                                consumer.accept(list);
                                list = TapSimplify.list();
                            }
                        }
                        if (EmptyKit.isNotEmpty(list)) {
                            consumer.accept(list);
                        }
                    }
                    hasResult = sqlStatement.getMoreResults();
                }
            }
            connection.commit();
        }
    }

    @Override
    @Deprecated
    public ExecuteResult<?> execute(String sql, TapSupplier<Connection> connectionSupplier) {
        ExecuteResult<?> executeResult;
        try (Connection connection = connectionSupplier.get();
             Statement sqlStatement = connection.createStatement()) {
            boolean isQuery = sqlStatement.execute(sql);
            if (isQuery) {
                try (ResultSet resultSet = sqlStatement.getResultSet()) {
                    List<DataMap> dataMaps = DbKit.getDataFromResultSet(resultSet);
                    processDataMap(dataMaps);
                    executeResult = new ExecuteResult<>().result(dataMaps);
                }
            } else {
                executeResult = new ExecuteResult<>().result((long) sqlStatement.getUpdateCount());
            }
            connection.commit();

        } catch (Throwable e) {
            executeResult = new ExecuteResult<>().error(e);
        }
        return executeResult;
    }

    private void processDataMap(DataMap dataMap) {
        if (processDataMapConsumer != null) {
            processDataMapConsumer.accept(dataMap);
        }
    }

    private void processDataMap(List<DataMap> dataMaps) throws SQLException {
        for (DataMap dataMap : dataMaps) {
            processDataMap(dataMap);
        }
    }

    @Override
    public ExecuteResult<?> call(String funcName, List<Map<String, Object>> params, TapSupplier<Connection> connectionSupplier) {
        if (EmptyKit.isEmpty(funcName)) {
            throw new IllegalArgumentException("procedure/function is null");
        }

        ExecuteResult<?> executeResult;

        funcName = funcName.trim();
        List<JdbcProcedureParam> outList = new ArrayList<>();

        try (Connection connection = connectionSupplier.get();
             CallableStatement callableStatement = createCallableStatement(funcName, params, connection, outList)) {
            if (callableStatement == null) {
                throw new RuntimeException("create callableStatement error");
            }
            boolean hasResult = callableStatement.execute();

            if (outList.isEmpty()) {
                executeResult = new ExecuteResult<>().result(true);
            } else {
                executeResult = new ExecuteResult<>().result(getOutputFromCall(outList, callableStatement, hasResult));
            }
        } catch (Throwable e) {
            executeResult = new ExecuteResult<>().error(new RuntimeException(String.format("Execute database procedure/function %s error, message: %s", funcName, e.getMessage()), e));
        }
        return executeResult;
    }

    private CallableStatement createCallableStatement(String funcName, List<Map<String, Object>> params, Connection connection, List<JdbcProcedureParam> outList) throws Exception {

        CallableStatement callableStatement;
        boolean hasReturn = hasReturn(params);
        StringBuilder callStr = new StringBuilder();

        if (hasReturn) {
            callStr.append("{?=call ")
                    .append(funcName)
                    .append("(")
                    .append(StringKit.copyString("?", params.size() - 1, ","))
                    .append(")}");
        } else {
            callStr.append("{call ")
                    .append(funcName)
                    .append("(")
                    .append(StringKit.copyString("?", params.size(), ","))
                    .append(")}");
        }

        callableStatement = connection.prepareCall(callStr.toString());

        if (callableStatement == null) {
            return null;
        }

        setCallableStatementParameters(callableStatement, params, outList, connection);

        return callableStatement;
    }

    protected void setCallableStatementParameters(CallableStatement callableStatement, List<Map<String, Object>> params, List<JdbcProcedureParam> outList, Connection connection) throws Exception {
        if (callableStatement == null || params == null || params.size() == 0) {
            return;
        }
        for (int paramIndex = 1; paramIndex <= params.size(); paramIndex++) {
            Map<String, Object> paramMap = params.get(paramIndex - 1);
            if (paramMap == null || paramMap.isEmpty()) {
                throw new Exception("parameter wrong: cannot be empty");
            }

            Object objMode = paramMap.get(PARAMS_MODE);
            String mode = objMode == null ? MODE_IN : objMode.toString();
            Object value = paramMap.get(PARAMS_VALUE);
            Object objName = paramMap.get(PARAMS_NAME);
            String name = objName == null ? "param" + paramIndex : objName.toString().trim();
            Object objType = paramMap.get(PARAMS_TYPE);
            String type = objType == null ? "" : objType.toString();
            int jdbcType = type2JdbcType(type);

            if (mode.equalsIgnoreCase(MODE_IN) || mode.equalsIgnoreCase(MODE_IN_OUT)) {
                //帮我完善不同的setObject
                if (jdbcType == Types.TIMESTAMP) {
                    String dateFormat = DateUtil.determineDateFormat(value.toString());
                    if (dateFormat != null) {
                        Instant instant = LocalDateTime.parse(value.toString(), DateTimeFormatter.ofPattern(dateFormat))
                                .atZone(ZoneId.systemDefault())
                                .toInstant();
                        callableStatement.setTimestamp(paramIndex, Timestamp.from(instant));
                    } else {
                        callableStatement.setTimestamp(paramIndex, Timestamp.valueOf(value.toString()));
                    }
                } else {
                    callableStatement.setObject(paramIndex, value, jdbcType);
                }
            }

            if (mode.equalsIgnoreCase(MODE_OUT) || mode.equalsIgnoreCase(MODE_IN_OUT)) {
                JdbcProcedureParam jdbcProcedureParam = new JdbcProcedureParam(name, paramIndex, type, jdbcType);
                outList.add(jdbcProcedureParam);
                callableStatement.registerOutParameter(paramIndex, jdbcType);
            }
            if (mode.equalsIgnoreCase(MODE_RETURN)) {
                JdbcProcedureParam jdbcProcedureParam = new JdbcProcedureParam(name, 1, type, jdbcType);
                outList.add(jdbcProcedureParam);
                callableStatement.registerOutParameter(1, jdbcType);
            }
        }
    }

    private Object getOutputFromCall(List<JdbcProcedureParam> outList, CallableStatement callableStatement, boolean hasResult) throws Exception {
        if (outList == null || outList.isEmpty() || callableStatement == null) {
            return null;
        }
        Map<String, Object> res = new HashMap<>();
        int resIndex = 1;
        while (hasResult) {
            try (ResultSet resultSet = callableStatement.getResultSet()) {
                List<Map<String, Object>> list = TapSimplify.list();
                List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                while (resultSet.next()) {
                    DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                    processDataMap(dataMap);
                    list.add(dataMap);
                }
                res.put("result" + resIndex++, list);
            }
            hasResult = callableStatement.getMoreResults();
        }
        for (JdbcProcedureParam param : outList) {
            String name = param.getName();
            int paramIndex = param.getIndex();
            String type = param.getType();

            try {
                Object out = callableStatement.getObject(paramIndex);
                out = handleValue(out);
                res.put(name, out);
            } catch (SQLException e) {
                throw new Exception("get value {param name: " + name + ", param index: " + paramIndex + ", param type: " + type + "} error: " + e.getMessage());
            }
        }

        return res;
    }

    private Object handleValue(Object value) {
        try {
            if (value instanceof Clob) {
                value = DbKit.clobToString((Clob) value);
            } else if (value instanceof Blob) {
                value = DbKit.blobToBytes((Blob) value);
            } else if (value instanceof byte[]) {
                value = new String((byte[]) value);
            }
        } catch (Exception e) {
            throw new RuntimeException("handle value error: " + e.getMessage());
        }

        return value;
    }

    private boolean hasReturn(List<Map<String, Object>> params) {
        if (params == null) {
            return false;
        }
        return params.stream().anyMatch(v -> v != null && MODE_RETURN.equalsIgnoreCase(String.valueOf(v.get(PARAMS_MODE))));
    }

    protected int type2JdbcType(String type) {

        switch (type) {
            case "varchar":
            case "varchar2":
            case "nvarchar2":
            case "tinytext":
            case "mediumtext":
            case "longtext":
            case "text":
                return Types.VARCHAR;
            case "char":
            case "nchar":
            case "enum":
            case "set":
                return Types.CHAR;
            case "long":
                return Types.LONGVARCHAR;
            case "number":
            case "numeric":
                return Types.NUMERIC;
            case "raw":
            case "varbinary":
                return Types.VARBINARY;
            case "longraw":
                return Types.LONGVARBINARY;
            case "date":
            case "time":
            case "datetime":
            case "timestamp":
                return Types.TIMESTAMP;
            case "clob":
                return Types.CLOB;
            case "bit":
                return Types.BIT;
            case "tinyint":
            case "bool":
            case "boolean":
                return Types.TINYINT;
            case "smallint":
                return Types.SMALLINT;
            case "mediumint":
            case "int":
            case "integer":
                return Types.INTEGER;
            case "bigint":
                return Types.BIGINT;
            case "float":
                return Types.FLOAT;
            case "double":
                return Types.DOUBLE;
            case "decimal":
                return Types.DECIMAL;
            case "binary":
                return Types.BINARY;
            case "tinyblob":
            case "mediumblob":
            case "longblob":
            case "blob":
                return Types.BLOB;
            default:
                throw new IllegalArgumentException("Not supported:" + type);
        }
    }

}
