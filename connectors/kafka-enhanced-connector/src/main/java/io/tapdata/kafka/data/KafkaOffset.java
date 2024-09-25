package io.tapdata.kafka.data;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Kafka 增量偏移信息
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/26 20:25 Create
 */
public class KafkaOffset extends HashMap<String, KafkaTopicOffset> implements Serializable, Cloneable {

    public void setOffset(String topic, Integer partition, Long offset) {
        this.computeIfAbsent(topic, k -> new KafkaTopicOffset()).setOffset(partition, offset);
    }

    public void setOffset(ConsumerRecord<?, ?> consumerRecord) {
        // +1 让重启时跳过已同步的数据
        setOffset(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset() + 1);
    }

    @Override
    public KafkaOffset clone() {
        KafkaOffset clone = new KafkaOffset();
        this.forEach((topic, offset) -> clone.put(topic, offset.clone()));
        return clone;
    }
}
