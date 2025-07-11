package io.tapdata.connector.elasticsearch;

import io.tapdata.constant.DbTestItem;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestConnectionEx;
import io.tapdata.pdk.apis.exception.testItem.TapTestHostPortEx;
import io.tapdata.util.NetUtil;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

import static io.tapdata.base.ConnectorBase.testItem;

public class ElasticsearchTest {

    private final ElasticsearchConfig elasticsearchConfig;
    private final ElasticsearchHttpContext elasticsearchHttpContext;

    public ElasticsearchTest(ElasticsearchConfig elasticsearchConfig) {
        this.elasticsearchConfig = elasticsearchConfig;
        this.elasticsearchHttpContext = new ElasticsearchHttpContext(elasticsearchConfig);
    }

    public TestItem testHostPort() {
        try {
            NetUtil.validateHostPortWithSocket(elasticsearchConfig.getHost(), elasticsearchConfig.getPort());
            return testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY);
        } catch (IOException e) {
            return new TestItem(DbTestItem.HOST_PORT.getContent(), new TapTestHostPortEx(e, elasticsearchConfig.getHost(), String.valueOf(elasticsearchConfig.getPort())), TestItem.RESULT_FAILED);
        }
    }

    public TestItem testConnect() {
        try {
            if (elasticsearchHttpContext.getElasticsearchClient().ping(RequestOptions.DEFAULT)) {
                return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
            }
        } catch (Exception e) {
            return new TestItem(TestItem.ITEM_CONNECTION, new TapTestConnectionEx(e), TestItem.RESULT_FAILED);
        }
        return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Elasticsearch client ping failed!");
    }

    public void close() {
        try {
            elasticsearchHttpContext.finish();
        } catch (Exception ignored) {
        }
    }
}
