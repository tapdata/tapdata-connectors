package io.tapdata.connector.config;

import io.tapdata.connector.IConfigWithContext;
import io.tapdata.connector.tester.IStep;
import io.tapdata.connector.tester.items.HostPortTesterItem;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestHostPortEx;
import org.apache.commons.lang3.StringUtils;

import java.util.function.Consumer;

/**
 * 检测项-主机端口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/28 11:53 Create
 */
public interface ConnectionHostPort extends IConfigWithContext {
    String KEY_HOST = "host";
    String KEY_PORT = "port";

    default String getConnectionHost() {
        return connectionConfigGet(KEY_HOST, null);
    }

    default Integer getConnectionPort() {
        return connectionConfigGet(KEY_PORT, null);
    }

    default boolean testHostPort(TestItem item, Consumer<TestItem> consumer, ConnectionOptions options) {
        String host = getConnectionHost();
        Integer port = getConnectionPort();
        if (StringUtils.isBlank(host)) {
            item.setInformation("not configured 'host' yet");
        } else if (null == port || port <= 0) {
            item.setInformation("not configured 'port' yet");
        } else {
            try {
                String address = HostPortTesterItem.test(host, port);
                item.setResult(TestItem.RESULT_SUCCESSFULLY);
                item.setInformation(address);
            } catch (TapTestHostPortEx e) {
                item.setResult(TestItem.RESULT_FAILED);
                item.setTapTestItemException(e);
            }
        }

        return IStep.CHECK_ITEM_APPLY;
    }
}

