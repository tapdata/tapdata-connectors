package io.tapdata.kafka.serialization.json;

import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import com.alibaba.fastjson.serializer.SerializeWriter;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * 字节数组转 16 进制哈希
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/14 16:26 Create
 */
public class BinarySerializer implements ObjectSerializer {
    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        byte[] bytes = (byte[]) object;
        SerializeWriter writer = serializer.getWriter();
        writer.writeHex(bytes);
    }
}
