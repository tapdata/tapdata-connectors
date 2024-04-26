package io.tapdata.connector.kafka.util;

import io.tapdata.connector.kafka.data.KafkaStreamOffset;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/26 18:41 Create
 */
public class KafkaOffsetUtils {

	private KafkaOffsetUtils() {
	}

	public static <K, V> Set<TopicPartition> getTopicPartitions(KafkaConsumer<K, V> kafkaConsumer, AtomicBoolean consuming) throws InterruptedException {
		Set<TopicPartition> assignment = Collections.emptySet();
		while (assignment.isEmpty()) {
			if (!consuming.get()) {
				Thread.currentThread().interrupt();
				throw new InterruptedException("Interrupted while waiting for consumer thread");
			}

			kafkaConsumer.poll(Duration.ofSeconds(1L));
			assignment = kafkaConsumer.assignment();
		}

		return assignment;
	}

	public static <K, V> KafkaStreamOffset getStreamOffsetOfTimestamp(Long ts, KafkaConsumer<K, V> kafkaConsumer, AtomicBoolean consuming) throws InterruptedException {
		Set<TopicPartition> topicPartitions = getTopicPartitions(kafkaConsumer, consuming);

		// partition 中 的offset 为下次需要开始消费的偏移量（即包含 offset 所指的这个事件）
		KafkaStreamOffset streamOffset = new KafkaStreamOffset();
		if (null == ts) {
			// 从当前时间开始
			kafkaConsumer.beginningOffsets(topicPartitions, Duration.ofSeconds(1L)).forEach(
				(k, v) -> streamOffset.addTopicOffset(k.topic(), k.partition(), v, null)
			);
		} else {
			// 初始化所有 topic-partition offset
			kafkaConsumer.endOffsets(topicPartitions, Duration.ofSeconds(1L)).forEach(
				(k, v) -> streamOffset.addTopicOffset(k.topic(), k.partition(), v, ts)
			);

			// 如果指定时间之后有数据，则替换 offset 及 ts
			Map<TopicPartition, Long> topicPartitionTs = new HashMap<>();
			topicPartitions.forEach(topicPartition -> topicPartitionTs.put(topicPartition, ts));
			kafkaConsumer.offsetsForTimes(topicPartitionTs).forEach(
				(k, v) -> {
					if (null == v) return;
					streamOffset.addTopicOffset(k.topic(), k.partition(), v.offset(), v.timestamp());
				}
			);
		}
		return streamOffset;
	}

	public static <K, V> KafkaStreamOffset setConsumerByOffset(KafkaConsumer<K, V> kafkaConsumer, List<String> tableList, Object offset, AtomicBoolean consuming) throws InterruptedException {
		KafkaStreamOffset streamOffset;
		if (offset instanceof KafkaStreamOffset) {
			streamOffset = (KafkaStreamOffset) offset;
			Map<TopicPartition, Long> partitions = new HashMap<>();
			streamOffset.forEach((k, v) -> {
				if (null == v) return;
				TopicPartition topicPartition = new TopicPartition(k, v.getPartition());
				partitions.put(topicPartition, v.getOffset());
			});
			kafkaConsumer.assign(partitions.keySet());
		} else {
			kafkaConsumer.subscribe(tableList);
			streamOffset = KafkaOffsetUtils.getStreamOffsetOfTimestamp((Long) offset, kafkaConsumer, consuming);
		}

		seekConsumerByOffset(kafkaConsumer, streamOffset);
		return streamOffset;
	}

	public static <K, V> void seekConsumerByOffset(KafkaConsumer<K, V> kafkaConsumer, KafkaStreamOffset streamOffset) {
		Optional.ofNullable(streamOffset).ifPresent(m -> m.forEach((k, v) -> {
			TopicPartition topicPartition = new TopicPartition(k, v.getPartition());
			kafkaConsumer.seek(topicPartition, v.getOffset());
		}));
	}

}
