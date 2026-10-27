package io.tapdata.common;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.TapSupplier;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SqlExecuteCommandFunction {

    private static final SqlExecutor defaultSqlExecutor = new DefaultSqlExecutor();

    private final SqlExecutor sqlExecutor;

    public SqlExecuteCommandFunction(SqlExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }

    @Deprecated
    public void executeSqlCommand(TapConnectorContext tapConnectorContext, TapExecuteCommand tapExecuteCommand, TapSupplier<Connection> connectionSupplier, Consumer<ExecuteResult> executeResultConsumer) {
        executeCommand(sqlExecutor, tapExecuteCommand, connectionSupplier, executeResultConsumer);
    }

    public void executeSqlCommand(TapConnectorContext tapConnectorContext, TapExecuteCommand tapExecuteCommand, TapSupplier<Connection> connectionSupplier, Supplier<Boolean> aliveSupplier, Consumer<ExecuteResult> executeResultConsumer) {
        executeCommand(sqlExecutor, tapExecuteCommand, connectionSupplier, aliveSupplier, executeResultConsumer);
    }

    @Deprecated
    public static void executeCommand(TapConnectorContext tapConnectorContext, TapExecuteCommand tapExecuteCommand, TapSupplier<Connection> connectionSupplier, Consumer<ExecuteResult> executeResultConsumer) {
        executeCommand(defaultSqlExecutor, tapExecuteCommand, connectionSupplier, executeResultConsumer);
    }

    @Deprecated
    private static void executeCommand(SqlExecutor sqlExecutor, TapExecuteCommand tapExecuteCommand, TapSupplier<Connection> connectionSupplier, Consumer<ExecuteResult> executeResultConsumer) {
        // Keep the deprecated entry point on the same code path so queryTimeoutSeconds and batchSize
        // are honored; legacy callers have no cancellation supplier, so the command never cancels.
        executeCommand(sqlExecutor, tapExecuteCommand, connectionSupplier, () -> true, executeResultConsumer);
    }

    public static void executeCommand(TapConnectorContext tapConnectorContext, TapExecuteCommand tapExecuteCommand, TapSupplier<Connection> connectionSupplier, Supplier<Boolean> aliveSupplier, Consumer<ExecuteResult> executeResultConsumer) {
        executeCommand(defaultSqlExecutor, tapExecuteCommand, connectionSupplier, aliveSupplier, executeResultConsumer);
    }

    private static void executeCommand(SqlExecutor sqlExecutor, TapExecuteCommand tapExecuteCommand, TapSupplier<Connection> connectionSupplier, Supplier<Boolean> aliveSupplier, Consumer<ExecuteResult> executeResultConsumer) {
        try {
            Map<String, Object> params = tapExecuteCommand.getParams();
            String command = tapExecuteCommand.getCommand();
            switch (command) {
                case "execute":
                case "executeQuery":
                    String sql = (String) params.get("sql");
                    int batchSize = SqlCommandParams.getInteger(params, "batchSize", 1000);
                    int queryTimeoutSeconds = SqlCommandParams.getQueryTimeoutSeconds(params);
                    sqlExecutor.execute(sql, connectionSupplier,
                            list -> executeResultConsumer.accept(new ExecuteResult().result(list)),
                            aliveSupplier, batchSize, queryTimeoutSeconds);
                    break;
                case "call":
                    String funcName = (String) params.get("funcName");
                    List<Map<String, Object>> callParams = (List<Map<String, Object>>) params.get("params");
                    executeResultConsumer.accept(sqlExecutor.call(funcName, callParams, connectionSupplier));
                    break;
                default:
                    executeResultConsumer.accept(new ExecuteResult<>().error(new IllegalArgumentException("Not supported command: " + command)));
            }
        } catch (Throwable e) {
            executeResultConsumer.accept(new ExecuteResult<>().error(e));
        }
    }
}
