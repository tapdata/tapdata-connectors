package io.tapdata.connector.tester.items;

import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestHostPortEx;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 测试项-集群地址
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/29 10:39 Create
 */
public interface ClusterURITesterItem {

    static void test(String clusterURI, TestItem item, Consumer<TestItem> consumer) {
        boolean hasFailed = false;
        List<String> results = new ArrayList<>();
        String errorItemName = "Invalid Host Port";
        for (String server : clusterURI.split(",")) {
            String[] arr = server.split(":");
            switch (arr.length) {
                case 1:
                    hasFailed = true;
                    consumer.accept(new TestItem(errorItemName, TestItem.RESULT_FAILED, String.format("not configured 'port' yet: %s", server)));
                    break;
                case 2:
                    String host = arr[0].trim();
                    String port = arr[1].trim();
                    try {
                        results.add(HostPortTesterItem.test(host, Integer.parseInt(port)));
                    } catch (TapTestHostPortEx e) {
                        hasFailed = true;
                        consumer.accept(new TestItem(errorItemName, e, TestItem.RESULT_FAILED));
                    } catch (NumberFormatException e) {
                        hasFailed = true;
                        consumer.accept(new TestItem(errorItemName, new TapTestHostPortEx(e, host, port), TestItem.RESULT_FAILED));
                    }
                    break;
                default:
                    hasFailed = true;
                    consumer.accept(new TestItem(errorItemName, TestItem.RESULT_FAILED, "illegal argument: " + server));
                    break;
            }
        }

        if (results.isEmpty()) {
            item.setResult(TestItem.RESULT_FAILED);
            item.setInformation("all host ports are abnormal");
        } else if (hasFailed) {
            item.setResult(TestItem.RESULT_SUCCESSFULLY_WITH_WARN);
            item.setInformation(String.format("valid list: %s", String.join(",", results)));
        } else {
            item.setResult(TestItem.RESULT_SUCCESSFULLY);
            item.setInformation(null);
        }
    }
}
