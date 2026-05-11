package io.tapdata.connector.config;

import io.tapdata.connector.IConfigWithContext;

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

}
