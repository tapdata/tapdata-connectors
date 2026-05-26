package io.tapdata.kafka.data;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.*;

/**
 * Topic 增量偏移
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/26 18:43 Create
 */
public class KafkaTopicOffset extends HashMap<Integer, Long> implements Serializable, Cloneable {

    public void setOffset(Integer partition, Long offset) {
        put(partition, offset);
    }

    public KafkaTopicOffset append(Integer partition, Long offset) {
        put(partition, offset);
        return this;
    }

    @Override
    public KafkaTopicOffset clone() {
        KafkaTopicOffset clone = new KafkaTopicOffset();
        clone.putAll(this);
        return clone;
    }

    public Map<TopicPartition, Long> toPartitionOffsets(String topic) {
        Map<TopicPartition, Long> partitionOffset = new LinkedHashMap<>();
        for (Map.Entry<Integer, Long> entry : entrySet()) {
            partitionOffset.put(new TopicPartition(topic, entry.getKey()), entry.getValue());

        }
        return partitionOffset;
    }

}
