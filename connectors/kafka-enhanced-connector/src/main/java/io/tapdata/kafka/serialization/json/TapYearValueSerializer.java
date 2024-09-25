package io.tapdata.kafka.serialization.json;

import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import io.tapdata.entity.schema.value.TapYearValue;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/24 12:15 Create
 */
public class TapYearValueSerializer implements ObjectSerializer {
    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        TapYearValue tapVal = (TapYearValue) object;
        Object originValue = tapVal.getOriginValue();
        if (originValue instanceof Integer) {
            serializer.getWriter().write((int) originValue);
        } else if (null != originValue) {
            int i = Integer.parseInt(originValue.toString());
            serializer.getWriter().write(i);
        }
    }
}
