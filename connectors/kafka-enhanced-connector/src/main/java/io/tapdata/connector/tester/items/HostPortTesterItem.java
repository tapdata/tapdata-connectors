package io.tapdata.connector.tester.items;

import io.tapdata.connector.tester.IStep;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestHostPortEx;
import io.tapdata.util.NetUtil;

import java.util.function.Consumer;

/**
 * 检测项-主机端口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/28 11:53 Create
 */
public interface HostPortTesterItem {

    static String test(String host, int port) {
        String hostPort = String.format("%s:%d", host, port);
        try {
            NetUtil.validateHostPortWithSocket(host, port);
            return hostPort;
        } catch (Exception e) {
            throw new TapTestHostPortEx(hostPort, e, host, String.valueOf(port));
        }
    }

    static boolean test(String itemName, Consumer<TestItem> consumer, String host, int port) {
        try {
            NetUtil.validateHostPortWithSocket(host, port);
            consumer.accept(new TestItem(itemName, TestItem.RESULT_SUCCESSFULLY, String.format("%s:%d", host, port)));
            return IStep.CHECK_ITEM_APPLY;
        } catch (Exception e) {
            consumer.accept(new TestItem(itemName, new TapTestHostPortEx(e, host, String.valueOf(port)), TestItem.RESULT_FAILED));
            return IStep.CHECK_ITEM_SKIPPED;
        }
    }
}

