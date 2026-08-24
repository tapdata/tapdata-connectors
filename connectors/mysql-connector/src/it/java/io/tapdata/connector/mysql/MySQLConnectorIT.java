package io.tapdata.connector.mysql;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.ConnectorIT;
import io.tapdata.it.ConnectorTestContext;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * MySQL 连接器通用集成测试。
 * <p>
 * 继承 {@link ConnectorIT} 后自动运行框架内置的全部通用集成测试用例
 * （连接元数据、表 DDL、数据读写、事务、流式读取、命令等），
 * 对 MySQL 不支持的 ConnectorFunctions 能力自动跳过。
 * 必实现能力由 {@link #requiredCapabilities()} 主动声明（原则 3：声明式能力），
 * 框架据此校验：声明必实现但未实现、或已实现但无用例覆盖 → 测试失败。
 * <p>
 * 连接配置读取 src/it/resources/config/connection.json
 * （host/port/database/user/password），支持系统属性/环境变量覆盖
 * （如 -Dconnector.it.host=xxx 或 CONNECTOR_IT_HOST=xxx）。
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MySQLConnectorIT extends ConnectorIT {

    @Override
    protected Set<String> requiredCapabilities() {
        // MySQL 同时承担源与目标角色：声明对外承诺的全部能力。
        // 声明后若实现被移除或框架无对应用例，原则 3 校验用例将失败（不遗漏必实现接口、不漏测已实现能力）
        return Stream.of("connectionTest", "discoverSchema", "getTableNames",
                "createTableV2", "dropTable", "clearTable", "batchCount", "batchRead",
                "streamRead", "streamReadMultiConnection", "timestampToStreamOffset",
                "queryByAdvanceFilter", "countByPartitionFilter", "writeRecord", "afterInitialSync",
                "createIndex", "queryIndexes", "createConstraint", "queryConstraints", "dropConstraint",
                "newField", "alterFieldName", "alterFieldAttributes", "dropField",
                "errorHandle", "executeCommand", "executeCommandV2", "getTableInfo",
                "runRawCommand", "transactionBegin", "transactionCommit", "transactionRollback",
                "queryHashByAdvanceFilter", "exportEventSql").collect(Collectors.toSet());
    }

    @Override
    protected ConnectorTestContext createContext() throws Throwable {
        // 1. 创建连接器
        MysqlConnector connector = new MysqlConnector();

        // 2. 连接配置（与 src/test/resources 的占位 connection.json 隔离，避免资源覆盖冲突）
        DataMap config = readConnectionConfig("config/mysql-connection.json");

        // 3. 构建 NodeContext：specification + connectionConfig + nodeConfig + log
        TapConnectorContext nodeContext = new TapConnectorContext(
                null, config, DataMap.create().kv("enableTransaction", true), new TapLog());
        nodeContext.setStateMap(new TestStateMap());

        // 4. 注册能力（registerCapabilities 产物，基类据此动态检测并驱动用例）
        ConnectorFunctions functions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();
        connector.registerCapabilities(functions, codecRegistry);

        // 5. 组装测试上下文
        ConnectorTestContext ctx = ConnectorTestContext.builder()
                .connector(connector)
                .nodeContext(nodeContext)
                .connectorFunctions(functions)
                .codecRegistry(codecRegistry)
                .config(config)
                .log(new TapLog())
                .build();
        ctx.getLog().info("[IT] MySQL connection: host={}, port={}, database={}, user={}",
                config.getString("host"), config.getInteger("port"), config.getString("database"), config.getString("user"));
        return ctx;
    }

    @DisplayName("Test github action")
    @Test
    public void testGithubAction() {
        int result = 10*5;
        Assertions.assertEquals(50, result);
    }
}
