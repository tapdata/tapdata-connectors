package io.tapdata.connector.kafka.data;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Topic 增量偏移
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/26 18:43 Create
 */
public class KafkaTopicOffset extends HashMap<Integer, Long> implements Serializable, Cloneable {

    public void addPartitionOffset(Integer partition, Long offset) {
        put(partition, offset);
    }

    @Override
    public KafkaTopicOffset clone() {
        KafkaTopicOffset clone = new KafkaTopicOffset();
        clone.putAll(this);
        return clone;
    }

}
