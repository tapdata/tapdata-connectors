package io.tapdata.kafka.tester;

import io.tapdata.connector.tester.AbsStep;
import io.tapdata.connector.tester.ICommonStep;
import io.tapdata.connector.tester.IStep;
import io.tapdata.kafka.KafkaConfig;
import io.tapdata.kafka.KafkaTester;
import io.tapdata.kafka.service.KafkaAdminService;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestAuthEx;

import java.util.ArrayList;

/**
 * Kafka-基础测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/29 11:45 Create
 */
public class KafkaCommonStep extends AbsStep<KafkaConfig, KafkaTester> implements ICommonStep<KafkaConfig> {
    private KafkaAdminService adminService;

    public KafkaCommonStep(KafkaTester tester) {
        super(tester);
    }

    protected synchronized KafkaAdminService getAdminService() {
        if (null == adminService) {
            KafkaConfig config = tester.getConfig();
            this.adminService = new KafkaAdminService(config, config.tapConnectionContext().getLog());
        }
        return adminService;
    }

    @Override
    public boolean testVersion(TestItem testItem) {
        return IStep.CHECK_ITEM_SKIPPED; // Kafka 获取不到版本，不显示此测试项
    }

    @Override
    public boolean testConnection(TestItem testItem) {
        tester.getConfig().testClusterURI(testItem, itemConsumer(), options());
        if (TestItem.RESULT_FAILED != testItem.getResult()) {
            try {
                if (getAdminService().isClusterConnectable()) {
                    testItem.setResult(TestItem.RESULT_SUCCESSFULLY);
                    testItem.setInformation(null);
                } else {
                    testItem.setResult(TestItem.RESULT_FAILED);
                    testItem.setInformation("cluster is not connectable");
                }
            } catch (Exception e) {
                testItem.setResult(TestItem.RESULT_FAILED);
                testItem.setTapTestItemException(new TapTestAuthEx(String.format("cluster is not connectable: %s", e.getMessage()), e));
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return IStep.CHECK_ITEM_APPLY;
    }

    public boolean testRegistryConnection(TestItem testItem) {
        if (!tester.getConfig().getConnectionSchemaRegister()) {
            return IStep.CHECK_ITEM_SKIPPED;
        }
        getAdminService().testRegistryConnect(testItem);
        return CHECK_ITEM_APPLY;
    }

    @Override
    public boolean testInstanceUniqueId(TestItem testItem) {
        options().setInstanceUniqueId(StringKit.md5(String.join("|"
                , config().getConnectionClusterURI()
                , config().getConnectionSchemaMode().name()
                , config().getConnectionKeySerialization().name()
                , config().getConnectionValueSerialization().name()
        )));
        options().setNamespaces(new ArrayList<>());
        testItem.setResult(TestItem.RESULT_SUCCESSFULLY);
        testItem.setInformation(options().getInstanceUniqueId());
        return IStep.CHECK_ITEM_APPLY;
    }

    @Override
    public boolean testLogin(TestItem testItem) {
        return IStep.CHECK_ITEM_SKIPPED;
    }

    @Override
    public void close() throws Exception {
        if (null != adminService) {
            adminService.close();
        }
    }
}
