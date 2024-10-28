package io.tapdata.kafka.serialization;

import io.tapdata.kafka.utils.KafkaUtils;
import org.apache.kafka.common.serialization.Serializer;

/**
 * 二进制序列器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/24 20:21 Create
 */
public class BinarySerializer implements Serializer<Object> {

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data instanceof byte[]) {
            return (byte[]) data;
        } else if (data instanceof String) {
            return ((String) data).getBytes();
        } else {
            return KafkaUtils.toJsonBytes(data);
        }
    }
}
