package io.tapdata.connector.kafka.data;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

/**
 * Topic 增量偏移
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/26 18:43 Create
 */
public class KafkaTopicOffset implements Serializable, Cloneable {
	private Integer partition;
	private Long offset; // 代表下次消费事件从这个偏移量开始（包含这个事件）
	private Long ts;

	public KafkaTopicOffset() {
	}

	public KafkaTopicOffset(Integer partition, Long offset, Long ts) {
		this.partition = partition;
		this.offset = offset;
		this.ts = ts;
	}

	public Integer getPartition() {
		return partition;
	}

	public void setPartition(Integer partition) {
		this.partition = partition;
	}

	public Long getOffset() {
		return offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	@Override
	public KafkaTopicOffset clone() {
		return new KafkaTopicOffset(partition, offset, ts);
	}

	public static <K, V> KafkaTopicOffset of(ConsumerRecord<K, V> consumerRecord) {
		return new KafkaTopicOffset(consumerRecord.partition(), consumerRecord.offset() + 1, consumerRecord.timestamp());
	}

	public static KafkaTopicOffset of(Integer partition, Long offset, Long ts) {
		return new KafkaTopicOffset(partition, offset, ts);
	}
}
