package io.tapdata.connector.postgres;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DataMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * TAP-13194：clearTable 的 TRUNCATE CASCADE 由节点参数 cascadeTruncate 控制（默认关闭），dropTable 保持 CASCADE。
 */
class PostgresCascadeTruncateTest {

    private static final String TABLE = "parent_t";

    private PostgresConnector connector;
    private JdbcContext jdbcContext;
    private PostgresConfig config;
    private String schemaAndTable;

    @BeforeEach
    void setUp() throws Exception {
        connector = new PostgresConnector();
        jdbcContext = mock(JdbcContext.class);
        config = new PostgresConfig();
        config.setSchema("public");
        ReflectionTestUtils.setField(connector, "jdbcContext", jdbcContext);
        ReflectionTestUtils.setField(connector, "postgresConfig", config);
        ReflectionTestUtils.setField(connector, "commonDbConfig", config);
        ReflectionTestUtils.setField(connector, "tapLogger", mock(Log.class));
        doReturn(Collections.singletonList(TABLE)).when(jdbcContext).queryAllTables(anyList());
        schemaAndTable = ReflectionTestUtils.invokeMethod(connector, "getSchemaAndTable", TABLE);
    }

    private TapClearTableEvent clearEvent() {
        TapClearTableEvent event = new TapClearTableEvent();
        event.setTableId(TABLE);
        return event;
    }

    private TapDropTableEvent dropEvent() {
        TapDropTableEvent event = new TapDropTableEvent();
        event.setTableId(TABLE);
        return event;
    }

    @Test
    void u1ClearTableDefaultHasNoCascade() throws Exception {
        connector.clearTable(null, clearEvent());
        verify(jdbcContext).execute("truncate table " + schemaAndTable);
    }

    @Test
    void u1ClearTableSwitchOffHasNoCascade() throws Exception {
        config.setCascadeTruncate(false);
        connector.clearTable(null, clearEvent());
        verify(jdbcContext).execute("truncate table " + schemaAndTable);
    }

    @Test
    void u2ClearTableSwitchOnKeepsCascade() throws Exception {
        config.setCascadeTruncate(true);
        connector.clearTable(null, clearEvent());
        verify(jdbcContext).execute("truncate table " + schemaAndTable + " cascade");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void u3DropTableAlwaysCascade(boolean cascadeTruncate) throws Exception {
        config.setCascadeTruncate(cascadeTruncate);
        connector.dropTable(null, dropEvent());
        verify(jdbcContext).execute("drop table " + schemaAndTable + " cascade");
    }

    @Test
    void u4SkipWhenTableNotExists() throws Exception {
        doReturn(Collections.emptyList()).when(jdbcContext).queryAllTables(anyList());
        config.setCascadeTruncate(true);
        connector.clearTable(null, clearEvent());
        connector.dropTable(null, dropEvent());
        verify(jdbcContext, never()).execute(anyString());
    }

    @Test
    void u4ConfigLoadsFromNodeConfig() {
        PostgresConfig defaults = new PostgresConfig();
        defaults.load(new DataMap());
        Assertions.assertEquals(Boolean.FALSE, defaults.getCascadeTruncate());

        PostgresConfig enabled = new PostgresConfig();
        enabled.load(DataMap.create().kv("cascadeTruncate", true));
        Assertions.assertEquals(Boolean.TRUE, enabled.getCascadeTruncate());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "src/main/resources/spec_postgres.json",
            "../greenplum-connector/src/main/resources/spec_greenplum.json",
            "../opengauss-connector/src/main/resources/spec_opengauss.json",
            "../dws-connector/src/main/resources/spec_dws.json",
            "../polar-db-postgres-connector/src/main/resources/polar-db-postgres-spec.json",
            "../aliyun-rds-postgres-connector/src/main/resources/aliyun-rds-postgres-spec.json",
            "../aliyun-adb-postgres-connector/src/main/resources/aliyun-adb-postgres-spec.json",
            "../tencent-db-postgres-connector/src/main/resources/tencent-db-postgres-spec.json"
    })
    void u5SpecDeclaresCascadeTruncateDefaultFalse(String spec) throws Exception {
        Path path = Paths.get(spec);
        Assertions.assertTrue(Files.exists(path), "spec not found: " + path.toAbsolutePath());
        String json = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        com.alibaba.fastjson.JSONObject root = com.alibaba.fastjson.JSON.parseObject(json);
        com.alibaba.fastjson.JSONObject property = root.getJSONObject("configOptions")
                .getJSONObject("node").getJSONObject("properties").getJSONObject("cascadeTruncate");
        Assertions.assertNotNull(property, "cascadeTruncate missing in " + spec);
        Assertions.assertEquals("boolean", property.getString("type"));
        Assertions.assertEquals(Boolean.FALSE, property.getBoolean("default"));
        com.alibaba.fastjson.JSONObject messages = root.getJSONObject("messages");
        Assertions.assertEquals("en_US", messages.getString("default"));
        for (String locale : new String[]{"en_US", "zh_CN", "zh_TW"}) {
            com.alibaba.fastjson.JSONObject m = messages.getJSONObject(locale);
            Assertions.assertTrue(m.containsKey("cascadeTruncate"), locale + " title missing in " + spec);
            Assertions.assertTrue(m.containsKey("cascadeTruncateTooltip"), locale + " tooltip missing in " + spec);
        }
    }
}
