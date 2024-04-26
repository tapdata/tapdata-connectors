package io.tapdata.connector.kafka.data;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Kafka 增量偏移信息
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/26 20:25 Create
 */
public class KafkaStreamOffset extends HashMap<String, KafkaTopicOffset> implements Serializable, Cloneable {

	public void addTopicOffset(String topic, KafkaTopicOffset topicOffset) {
		this.put(topic, topicOffset);
	}

	public void addTopicOffset(String topic, Integer partition, Long offset, Long ts) {
		this.put(topic, KafkaTopicOffset.of(partition, offset, ts));
	}

	public void addTopicOffset(ConsumerRecord<byte[], byte[]> consumerRecord) {
		addTopicOffset(consumerRecord.topic(), KafkaTopicOffset.of(consumerRecord));
	}

	@Override
	public KafkaStreamOffset clone() {
		KafkaStreamOffset clone = new KafkaStreamOffset();
		this.forEach((topic, offset) -> clone.put(topic, offset.clone()));
		return clone;
	}
}
