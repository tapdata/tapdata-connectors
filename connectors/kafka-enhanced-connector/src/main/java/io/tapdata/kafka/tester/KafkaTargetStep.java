package io.tapdata.kafka.tester;

import io.tapdata.connector.tester.AbsStep;
import io.tapdata.connector.tester.IStep;
import io.tapdata.connector.tester.ITargetStep;
import io.tapdata.kafka.KafkaConfig;
import io.tapdata.kafka.KafkaTester;
import io.tapdata.pdk.apis.entity.TestItem;

/**
 * Kafka-目标测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/29 11:50 Create
 */
public class KafkaTargetStep extends AbsStep<KafkaConfig, KafkaTester> implements ITargetStep<KafkaConfig> {

    public KafkaTargetStep(KafkaTester tester) {
        super(tester);
    }

    @Override
    public boolean testWrite(TestItem item) {
        return IStep.CHECK_ITEM_SKIPPED;
    }

    @Override
    public void close() throws Exception {

    }
}
