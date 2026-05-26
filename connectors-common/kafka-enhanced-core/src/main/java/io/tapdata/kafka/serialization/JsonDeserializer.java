package io.tapdata.kafka.serialization;

import io.tapdata.kafka.utils.KafkaUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;

/**
 * Json 数据反序列化
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/5 14:07 Create
 */
public class JsonDeserializer implements Deserializer<Object> {

    @Override
    public Object deserialize(String topic, byte[] data) {
        String jsonStr = new String(data);
        if (jsonStr.startsWith("{")) {
            return KafkaUtils.parseJsonObject(data);
        }
        return KafkaUtils.parseList(jsonStr, Object.class);
    }
}
