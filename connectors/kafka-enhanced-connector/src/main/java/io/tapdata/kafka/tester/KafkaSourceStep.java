package io.tapdata.kafka.tester;

import io.tapdata.connector.tester.AbsStep;
import io.tapdata.connector.tester.ISourceStep;
import io.tapdata.connector.tester.IStep;
import io.tapdata.kafka.KafkaConfig;
import io.tapdata.kafka.KafkaTester;
import io.tapdata.pdk.apis.entity.TestItem;

/**
 * Kafka-源测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/29 11:48 Create
 */
public class KafkaSourceStep extends AbsStep<KafkaConfig, KafkaTester> implements ISourceStep<KafkaConfig> {

    public KafkaSourceStep(KafkaTester tester) {
        super(tester);
    }

    @Override
    public boolean testReadLog(TestItem item) {
        return IStep.CHECK_ITEM_SKIPPED;
    }

    @Override
    public boolean testRead(TestItem item) {
        return IStep.CHECK_ITEM_SKIPPED;
    }

    @Override
    public void close() throws Exception {

    }
}
