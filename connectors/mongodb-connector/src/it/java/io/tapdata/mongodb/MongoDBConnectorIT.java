package io.tapdata.mongodb;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.ConnectorIT;
import io.tapdata.it.ConnectorTestContext;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.TestInstance;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * MongoDB 连接器通用集成测试。
 * <p>
 * 继承 {@link ConnectorIT} 后自动运行框架内置的全部通用集成测试用例
 * （连接元数据、集合 DDL、数据读写、事务、流式读取、命令等），
 * 对 MongoDB 不支持的 ConnectorFunctions 能力自动跳过。
 * 必实现能力由 {@link #requiredCapabilities()} 主动声明（原则 3：声明式能力），
 * 框架据此校验：声明必实现但未实现、或已实现但无用例覆盖 → 测试失败。
 * <p>
 * 连接配置读取 src/it/resources/config/mongodb-connection.json
 * （uri/database），支持系统属性/环境变量覆盖
 * （如 -Dconnector.it.uri=xxx 或 CONNECTOR_IT_URI=xxx）。
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MongoDBConnectorIT extends ConnectorIT {

    @Override
    protected Set<String> requiredCapabilities() {
        // MongoDB 同时承担源与目标角色：声明对外承诺的全部能力（与 registerCapabilities 注册保持一致；
        // memoryFetcher 为引擎诊断钩子，由基类 ignoredCapabilities() 排除，不参与校验）
        return Stream.of("connectionTest", "discoverSchema", "getTableNames",
                "createTableV2", "dropTable", "batchCount", "batchRead", "streamRead",
                "timestampToStreamOffset", "queryByAdvanceFilter", "countByPartitionFilter",
                "writeRecord", "createIndex", "queryIndexes", "errorHandle",
                "executeCommand", "getTableInfo", "getReadPartitions", "queryFieldMinMaxValue",
                "transactionBegin", "transactionCommit", "transactionRollback").collect(Collectors.toSet());
    }

    @Override
    protected ConnectorTestContext createContext() throws Throwable {
        // 1. 创建连接器
        MongodbConnector connector = new MongodbConnector();

        // 2. 连接配置（URI 模式，与产品连接表单默认一致）
        DataMap config = readConnectionConfig("config/mongodb-connection.json");

        // 3. 构建 NodeContext：specification + connectionConfig + nodeConfig + log
        //    specification 提供 dataTypesMap，discoverSchema 的 tapType 自动填充依赖它
        TapConnectorContext nodeContext = new TapConnectorContext(
                loadSpecification("spec.json"), config, DataMap.create(), new TapLog());
        nodeContext.setStateMap(new TestStateMap());

        // 4. 注册能力（registerCapabilities 产物，基类据此动态检测并驱动用例）
        ConnectorFunctions functions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();
        connector.registerCapabilities(functions, codecRegistry);

        // 5. 组装测试上下文（特性开关：MongoDB NoSQL 特性适配）
        ConnectorTestContext ctx = ConnectorTestContext.builder()
                .connector(connector)
                .nodeContext(nodeContext)
                .connectorFunctions(functions)
                .codecRegistry(codecRegistry)
                .config(config)
                .log(new TapLog())
                // MongoDB createTableV2 幂等建集合固定返回 tableExists=false
                .createTableReportsTableExists(false)
                // schema-free：空集合无法推断字段，需先写入采样数据再 discoverSchema
                .schemaDiscoveryRequiresSampleData(true)
                // discoverSchema 会带出隐式 _id 字段
                .schemaAllowsExtraFields(true)
                // 主键为库自动生成的 _id，业务字段不被标记为主键
                .schemaPrimaryKeyStrict(false)
                // executeCommand 仅支持 execute/executeQuery/count/aggregate，不支持 ping
                .executeCommandSupportsPing(false)
                // queryFieldMinMaxValue 基于分区索引字段计算 min/max
                .fieldMinMaxRequiresPartitionIndex(true)
                .build();
        ctx.getLog().info("[IT] MongoDB connection: uri={}, database={}",
                config.getString("uri"), config.getString("database"));
        return ctx;
    }

    @Override
    protected List<Map<String, Object>> beforeWrite(List<Map<String, Object>> rows) {
        // unique 索引用例（idx_c_int）要求 c_int 唯一：随机值冲突时改为超出随机范围的序列值
        Set<Long> seen = new HashSet<>();
        long seq = 1_000_001L;
        for (Map<String, Object> row : rows) {
            long v = ((Number) row.get("c_int")).longValue();
            if (!seen.add(v)) {
                row.put("c_int", seq++);
            }
        }
        return rows;
    }

    @Override
    protected Map<String, Object> specialValueSamples() {
        // MongoDB 特有 BSON 类型样本：须与 registerCapabilities 注册的 ToTapValueCodec 一一对应
        // （U8 用例验证这些特殊值 wrap 后被连接器 codec 识别，而非 TapRawValue 兜底）
        Map<String, Object> samples = new LinkedHashMap<>();
        samples.put("obj_objectid", new ObjectId());
        samples.put("obj_binary", new Binary((byte) 0x80, new byte[]{1, 2, 3}));
        samples.put("obj_code", new Code("function() { return 1; }"));
        samples.put("obj_decimal128", Decimal128.parse("12345.6789"));
        samples.put("obj_symbol", new Symbol("sym"));
        samples.put("obj_bson_timestamp", new BsonTimestamp(1_700_000_000, 1));
        samples.put("obj_regex", new BsonRegularExpression("^tap.*", "i"));
        return samples;
    }

    @DisplayName("Test github action")
    @Test
    public void testGithubAction() {
        int result = 10*5;
        Assertions.assertEquals(50, result);
    }
}
