package io.tapdata.kafka.serialization.json;

import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapTimeValue;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/24 12:15 Create
 */
public class TapTimeValueSerializer implements ObjectSerializer {
    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        TapTimeValue tapVal = (TapTimeValue) object;
        String val = ConnectorBase.formatTapDateTime(tapVal.getValue(), "HH:mm:ss");
        serializer.getWriter().writeString(val);
    }
}
