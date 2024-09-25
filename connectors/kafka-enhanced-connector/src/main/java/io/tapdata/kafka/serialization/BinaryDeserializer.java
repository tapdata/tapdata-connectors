package io.tapdata.kafka.serialization;

import org.apache.kafka.common.serialization.Deserializer;

/**
 * 二进制数据反序列化
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/24 20:25 Create
 */
public class BinaryDeserializer implements Deserializer<byte[]> {

    @Override
    public byte[] deserialize(String topic, byte[] data) {
        return data;
    }
}
