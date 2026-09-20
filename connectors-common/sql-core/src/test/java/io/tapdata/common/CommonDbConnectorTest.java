package io.tapdata.common;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommonDbConnectorTest {

    CommonDbConnector common;
    TapTable tapTable;

    @BeforeEach
    void init(){
        common = new CommonDbConnector() {
            @Override
            public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
                return null;
            }

            @Override
            public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

            }

            @Override
            public void onStart(TapConnectionContext connectionContext) {

            }

            @Override
            public void onStop(TapConnectionContext connectionContext) {

            }
        };
        CommonDbConfig config = new CommonDbConfig();
        config.setMaxIndexNameLength(20);
        ReflectionTestUtils.setField(common, "commonDbConfig", config);

        tapTable = new TapTable("test");
        tapTable.add(new TapField("a1", "int"));
        tapTable.add(new TapField("a2", "varchar(20)"));
        tapTable.add(new TapField("a3", "varchar(20)"));
        tapTable.add(new TapField("aaaaaaaaaaaaaaaaaaa", "varchar(20)"));

    }

    @Test
    void testGetCreateIndexSqlShort() {
        TapIndex tapIndex = new TapIndex()
                .indexField(new TapIndexField().name("a1").fieldAsc(true))
                .indexField(new TapIndexField().name("a2").fieldAsc(false));
        String indexSQL = common.getCreateIndexSql(tapTable, tapIndex);
        Assertions.assertEquals("create index \"IDX_test_a1_a2\" on \"test\"(\"a1\" asc,\"a2\" desc)", indexSQL);
    }

    @Test
    void testGetCreateIndexSqlLong() {
        TapIndex tapIndex = new TapIndex().unique(true)
                .indexField(new TapIndexField().name("aaaaaaaaaaaaaaaaaaa").fieldAsc(true))
                .indexField(new TapIndexField().name("a2").fieldAsc(false));
        String indexSQL = common.getCreateIndexSql(tapTable, tapIndex);
        Matcher matcher = Pattern.compile("create unique index \"IDX_test_aaa([a-z0-9]{8})\" on \"test\"\\(\"aaaaaaaaaaaaaaaaaaa\" asc,\"a2\" desc\\)").matcher(indexSQL);
        Assertions.assertTrue(matcher.matches());
    }
}
