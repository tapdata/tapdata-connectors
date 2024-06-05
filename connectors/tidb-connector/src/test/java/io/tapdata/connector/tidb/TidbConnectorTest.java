package io.tapdata.connector.tidb;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.connector.tidb.util.HttpUtil;
import io.tapdata.connector.tidb.util.pojo.ChangeFeed;
import io.tapdata.connector.tidb.util.pojo.ReplicaConfig;
import io.tapdata.connector.tidb.util.pojo.Sink;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

public class TidbConnectorTest {
    @Test
    void testRegisterCapabilitiesCountByPartitionFilter(){
        TidbConnector tidbConnector = new TidbConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(tidbConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }
    @Test
    void testCreate() throws IOException {
        HttpUtil   httpUtil = new HttpUtil(new TapLog());
        ChangeFeed changefeed = new ChangeFeed();
        String changeFeedId = UUID.randomUUID().toString().replaceAll("-", "");
        if (Pattern.matches("^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$", changeFeedId)) {
            changefeed.setSinkUri("file:///tidbCDC888?protocol=canal-json");
            changefeed.setChangefeedId("simple-replication-task");
            changefeed.setForceReplicate(true);
            changefeed.setSyncDdl(true);
            JSONObject jsonObject = new JSONObject();
            List<String> tableList = new ArrayList();
            tableList.add("*");
            List rules = new ArrayList();
            for(String table:tableList) {
                String rule = "test"+"."+table;
                rules.add(rule);
            }
            jsonObject.put("rules", rules.toArray());
            ReplicaConfig replicaConfig = new ReplicaConfig();
            replicaConfig.setFilter(jsonObject);
            Sink sink = new Sink();
            sink.setDateSeparator("none");
            sink.setProtocol("canal-json");
            replicaConfig.setSink(sink);
            changefeed.setReplicaConfig(replicaConfig);
            httpUtil.createChangeFeed(changefeed, "123.60.132.254:8300");
        }
    }


    @Test
    void testDelete() throws IOException {
        HttpUtil   httpUtil = new HttpUtil(new TapLog());
        httpUtil.deleteChangeFeed("simple-replication-task9", "127.0.0.1:8300");
    }


    @Test
    void queryChangefeedlist() throws IOException {
        HttpUtil httpUtil = new HttpUtil(new TapLog());
        httpUtil.queryChangeFeedsList("123.60.132.254:8300");
    }
}
