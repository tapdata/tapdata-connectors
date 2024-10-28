package io.tapdata.connector.config;

import io.tapdata.connector.IConfigWithContext;
import io.tapdata.connector.tester.IStep;
import io.tapdata.connector.tester.items.ClusterURITesterItem;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

/**
 * 测试项-集群地址
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/29 10:39 Create
 */
public interface ConnectionClusterURI extends IConfigWithContext {
    String KEY_CLUSTER_URI = "clusterURI";

    default String getConnectionClusterURI() {
        return connectionConfigGet(KEY_CLUSTER_URI, null);
    }

    default boolean testClusterURI(TestItem item, Consumer<TestItem> consumer, ConnectionOptions options) {
        String clusterURI = getConnectionClusterURI();
        if (null == clusterURI) {
            item.setResult(TestItem.RESULT_FAILED);
            item.setInformation(String.format("not configured '%s' yet", KEY_CLUSTER_URI));
        } else {
            options.connectionString(clusterURI);
            ClusterURITesterItem.test(clusterURI, item, consumer);
        }
        return IStep.CHECK_ITEM_APPLY;
    }
}
