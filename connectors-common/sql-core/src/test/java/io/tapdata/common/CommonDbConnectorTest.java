package io.tapdata.common;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.function.Consumer;

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
        config.setMaxIndexNameLength(32);
        ReflectionTestUtils.setField(common, "commonDbConfig", config);

        tapTable = new TapTable("test");
        tapTable.add(new TapField("a1", "int"));
        tapTable.add(new TapField("a2", "varchar(20)"));
        tapTable.add(new TapField("a3", "varchar(20)"));
    }

    @Test
    public void testGetCreateIndexSql() {
//        common.getCreateIndexSql()
    }
}
