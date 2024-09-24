package io.tapdata.kafka;

import io.tapdata.connector.tester.Tester;
import io.tapdata.kafka.tester.KafkaCommonStep;
import io.tapdata.kafka.tester.KafkaSourceStep;
import io.tapdata.kafka.tester.KafkaTargetStep;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

/**
 * Kafka 连接测试实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/28 17:07 Create
 */
public class KafkaTester extends Tester<KafkaConfig> {
    public KafkaTester(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        super(KafkaConfig.valueOf(connectionContext), consumer);
    }

    @Override
    protected KafkaCommonStep openCommon() {
        return new KafkaCommonStep(this);
    }

    @Override
    protected KafkaSourceStep openSource() {
        return new KafkaSourceStep(this);
    }

    @Override
    protected KafkaTargetStep openTarget() {
        return new KafkaTargetStep(this);
    }
}
