package io.tapdata.connector.config;

import io.tapdata.connector.IConfigWithContext;
import io.tapdata.connector.tester.IStep;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 检测项-数据源实例信息
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/25 15:05 Create
 */
public interface ConnectionDatasourceInstanceInfo extends IConfigWithContext {

    default String getDatasourceInstanceTag() {
        if (this instanceof ConnectionClusterURI) {
            return ((ConnectionClusterURI) this).getConnectionClusterURI();
        } else if (this instanceof ConnectionHostPort) {
            return String.format("%s:%d", ((ConnectionHostPort) this).getConnectionHost(), ((ConnectionHostPort) this).getConnectionPort());
        }
        return null;
    }

    default String toDatasourceInstanceId(String tag) {
        return StringKit.md5(tag);
    }

    default String getDatasourceInstanceId() {
        String tag = getDatasourceInstanceTag();
        if (null != tag) {
            return toDatasourceInstanceId(tag);
        }
        return null;
    }

    default boolean testDatasourceInstanceInfo(TestItem item, Consumer<TestItem> consumer, ConnectionOptions options) {
        String tag = getDatasourceInstanceTag();
        if (null != tag) {
            Map<String, String> datasourceInstanceInfo = new HashMap<>();
            datasourceInstanceInfo.put("tag", tag);
            datasourceInstanceInfo.put("id", toDatasourceInstanceId(tag));
            options.setDatasourceInstanceInfo(datasourceInstanceInfo);

            item.setResult(TestItem.RESULT_SUCCESSFULLY);
            item.setInformation(tag);
        }
        return IStep.CHECK_ITEM_APPLY;
    }
}

