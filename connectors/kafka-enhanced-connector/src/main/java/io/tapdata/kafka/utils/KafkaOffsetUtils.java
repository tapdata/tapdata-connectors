package io.tapdata.kafka.utils;

import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.data.KafkaOffset;
import io.tapdata.kafka.data.KafkaTopicOffset;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.ObjLongConsumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/26 18:41 Create
 */
public interface KafkaOffsetUtils {

    static <K, V> List<Long> beginOffsets(KafkaConsumer<K, V> kafkaConsumer, Collection<TopicPartition> topicPartitions) {
        kafkaConsumer.assign(topicPartitions);
        kafkaConsumer.seekToBeginning(topicPartitions);

        List<Long> offsets = new ArrayList<>(topicPartitions.size());
        for (int i = 0; i < topicPartitions.size(); i++) offsets.add(0L);

        for (TopicPartition topicPartition : topicPartitions) {
            int partition = topicPartition.partition();
            long position = kafkaConsumer.position(topicPartition);
            offsets.set(partition, position);
        }
        return offsets;
    }

    static <K, V> KafkaOffset getKafkaOffset(KafkaConsumer<K, V> kafkaConsumer, Collection<TopicPartition> topicPartitions, boolean isEarliest) {
        KafkaOffset streamOffset = new KafkaOffset();
        eachOffsetOfPartitions(kafkaConsumer, topicPartitions, isEarliest, (topicPartition, value) -> {
            KafkaTopicOffset kafkaTopicOffset = streamOffset.computeIfAbsent(topicPartition.topic(), s -> new KafkaTopicOffset());
            int partition = topicPartition.partition();
            long position = kafkaConsumer.position(topicPartition);
            kafkaTopicOffset.setOffset(partition, position);

        });
        return streamOffset;
    }

    static <K, V> void eachOffsetOfPartitions(KafkaConsumer<K, V> kafkaConsumer, Collection<TopicPartition> topicPartitions, boolean isEarliest, ObjLongConsumer<TopicPartition> callback) {
        kafkaConsumer.assign(topicPartitions);
        if (isEarliest) {
            kafkaConsumer.seekToBeginning(topicPartitions);
        } else {
            kafkaConsumer.seekToEnd(topicPartitions);
        }

        for (TopicPartition topicPartition : topicPartitions) {
            long position = kafkaConsumer.position(topicPartition);
            callback.accept(topicPartition, position);
        }
    }

    static <K, V> KafkaOffset timestamp2Offset(KafkaConsumer<K, V> kafkaConsumer, Collection<TopicPartition> topicPartitions, Long ts) {
        // partition 中 的 offset 为下次需要开始消费的偏移量（即包含 offset 所指的这个事件）
        KafkaOffset offset = getKafkaOffset(kafkaConsumer, topicPartitions, false);
        // 如果指定时间之后有数据，则替换 offset
        if (null != ts) {
            Map<TopicPartition, Long> topicPartitionTs = new HashMap<>();
            topicPartitions.forEach(topicPartition -> topicPartitionTs.put(topicPartition, ts));
            kafkaConsumer.offsetsForTimes(topicPartitionTs).forEach((k, v) -> {
                if (null == v) return;
                offset.setOffset(k.topic(), k.partition(), v.offset());
            });
        }
        return offset;
    }

    static <K, V> void setKafkaOffset(KafkaConsumer<K, V> kafkaConsumer, KafkaOffset streamOffset) {
        Map<TopicPartition, Long> partitionOffset = new LinkedHashMap<>();
        streamOffset.forEach((topic, partitions) ->
            partitions.forEach((partition, offset) ->
                partitionOffset.put(new TopicPartition(topic, partition), offset)
            )
        );
        kafkaConsumer.assign(partitionOffset.keySet());
        partitionOffset.forEach(kafkaConsumer::seek);
    }

    static void fillPartitions(IKafkaService service, Collection<String> tables, KafkaOffset streamOffset, boolean isEarliest) throws ExecutionException, InterruptedException {
        // 补全分区信息（如：全量过程为主题添加了分区）
        List<TopicPartition> need2UpdatePartitions = new ArrayList<>();
            Collection<TopicPartition> topicPartitions = service.getAdminService().getTopicPartitions(tables);
            topicPartitions.forEach(topicPartition -> {
                KafkaTopicOffset kafkaTopicOffset = streamOffset.get(topicPartition.topic());
                if (null == kafkaTopicOffset || !kafkaTopicOffset.containsKey(topicPartition.partition())) {
                    need2UpdatePartitions.add(topicPartition);
                }
            });
            if (!need2UpdatePartitions.isEmpty()) {
                try (KafkaConsumer<?, ?> kafkaConsumer = new KafkaConsumer<>(service.getConfig().buildConsumerConfig(isEarliest))) {
                    KafkaOffset kafkaOffset = KafkaOffsetUtils.getKafkaOffset(kafkaConsumer, need2UpdatePartitions, isEarliest);
                    kafkaOffset.forEach((k, v) -> v.forEach((p, o) -> streamOffset.setOffset(k, p, o)));
                }
            }
    }

}
