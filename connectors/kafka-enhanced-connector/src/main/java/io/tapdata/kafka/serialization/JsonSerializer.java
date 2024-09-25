package io.tapdata.kafka.serialization;

import io.tapdata.kafka.utils.KafkaUtils;
import org.apache.kafka.common.serialization.Serializer;

/**
 * JSON 数据序列化
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/5 14:05 Create
 */
public class JsonSerializer implements Serializer<Object> {

    @Override
    public byte[] serialize(String topic, Object data) {
        return KafkaUtils.toJsonBytes(data);
    }
}
