package io.tapdata.kafka.serialization.json;

import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.schema.value.DateTime;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/24 12:14 Create
 */
public class DateTimeSerializer implements ObjectSerializer {
    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        DateTime tapVal = (DateTime) object;
        String val = ConnectorBase.formatTapDateTime(tapVal, "yyyy-MM-dd HH:mm:ss.SSSSSS");
        serializer.getWriter().writeString(val);
    }
}
