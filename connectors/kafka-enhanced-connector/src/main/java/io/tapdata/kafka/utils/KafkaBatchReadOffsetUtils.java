package io.tapdata.kafka.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.exception.TapPdkConfigEx;
import io.tapdata.kafka.KafkaConfig;
import io.tapdata.kafka.data.KafkaOffset;
import io.tapdata.kafka.data.KafkaTopicOffset;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Optional;

/**
 * 解决全量同步切换到增量时数据有重复问题
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/8 18:04 Create
 */
public interface KafkaBatchReadOffsetUtils {
    String BATCH_READ_END_OFFSET = "batchReadEndOffset";

    static void toStateMap(KafkaConfig kafkaConfig, KafkaOffset kafkaOffset) {
        if (!kafkaConfig.stateMapSet(BATCH_READ_END_OFFSET, serialize(kafkaOffset))) {
            kafkaConfig.tapConnectionContext().getLog().warn("Can't set offset to stateMap");
        }
    }

    static KafkaOffset fromStateMap(KafkaConfig kafkaConfig) {
        return Optional.ofNullable((String) kafkaConfig.stateMapGet(BATCH_READ_END_OFFSET))
            .map(KafkaBatchReadOffsetUtils::deserialize)
            .orElse(null);
    }

    static <K, V> KafkaTopicOffset topicOffsetFromStateMap(String topic, KafkaConfig kafkaConfig, KafkaConsumer<K, V> consumer, Collection<TopicPartition> partitions) {
        return Optional.ofNullable(fromStateMap(kafkaConfig))
            .map(kafkaOffset -> kafkaOffset.get(topic))
            .orElseGet(() -> {
                try {
                    KafkaOffset kafkaOffset = KafkaOffsetUtils.getKafkaOffset(consumer, partitions, false);
                    return kafkaOffset.get(topic);
                } catch (Exception e) {
                    throw new TapPdkConfigEx("Failed to find end offset for " + topic, e);
                }
            });
    }

    static String serialize(KafkaOffset kafkaOffset) {
        return JSON.toJSONString(kafkaOffset);
    }

    static KafkaOffset deserialize(String jsonStr) {
        JSONObject offsetJson = JSON.parseObject(jsonStr);
        KafkaOffset kafkaOffset = new KafkaOffset();
        for (String k : offsetJson.keySet()) {
            JSONObject value = offsetJson.getJSONObject(k);
            for (String p : value.keySet()) {
                kafkaOffset.setOffset(k, Integer.parseInt(p), value.getLongValue(p));
            }
        }
        return kafkaOffset;
    }
}
