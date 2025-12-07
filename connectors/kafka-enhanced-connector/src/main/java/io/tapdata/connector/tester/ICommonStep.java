package io.tapdata.connector.tester;

import io.tapdata.connector.IConfigWithContext;
import io.tapdata.connector.config.ConnectionClusterURI;
import io.tapdata.connector.config.ConnectionDatasourceInstanceInfo;
import io.tapdata.connector.config.ConnectionHostPort;
import io.tapdata.pdk.apis.entity.TestItem;

/**
 * 基础测试项
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/28 19:00 Create
 */
public interface ICommonStep<C extends IConfigWithContext> extends IStep<C> {

    default boolean testVersion(TestItem testItem) {
        return CHECK_ITEM_APPLY;
    }

    default boolean testConnection(TestItem testItem) {
        C config = config();
        if (config instanceof ConnectionClusterURI) {
            return ((ConnectionClusterURI) config).testClusterURI(testItem, itemConsumer(), options());
        } else if (config instanceof ConnectionHostPort) {
            return ((ConnectionHostPort) config).testHostPort(testItem, itemConsumer(), options());
        }
        return CHECK_ITEM_APPLY;
    }

    default boolean testRegistryConnection(TestItem testItem) {
        return CHECK_ITEM_APPLY;
    }

    default boolean testInstanceUniqueId(TestItem testItem) {
        return CHECK_ITEM_APPLY;
    }

    default boolean testDatasourceInstanceInfo(TestItem testItem) {
        C config = config();
        if (config instanceof ConnectionDatasourceInstanceInfo) {
            return ((ConnectionDatasourceInstanceInfo) config).testDatasourceInstanceInfo(testItem, itemConsumer(), options());
        }
        return CHECK_ITEM_APPLY;
    }

    default boolean testLogin(TestItem testItem) {
        return CHECK_ITEM_APPLY;
    }

    default boolean test() {
        return checkItem(TestItem.ITEM_VERSION, this::testVersion)
                && checkItem(TestItem.ITEM_CONNECTION, this::testConnection)
                && checkItem(TestItem.ITEM_CONNECTION, this::testRegistryConnection)
                && checkItem(TestItem.ITEM_INSTANCE_UNIQUE_ID, this::testInstanceUniqueId)
                && checkItem(TestItem.ITEM_DATASOURCE_INSTANCE_INFO, this::testDatasourceInstanceInfo)
                && checkItem(TestItem.ITEM_LOGIN, this::testLogin);
    }
}
