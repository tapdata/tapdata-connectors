package io.tapdata.mongodb.reader.cdc.v3;

import io.tapdata.mongodb.reader.cdc.Acceptor;
import io.tapdata.mongodb.reader.v3.MongoV3StreamOffset;
import io.tapdata.mongodb.reader.v3.TapEventOffset;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/9 11:06 Create
 * @description
 */
public abstract class V3Accept<T extends V3Accept<T, Consumer>, Consumer extends TapStreamReadConsumer<?, Object>>  implements Acceptor<T, TapEventOffset, Consumer> {
    protected Map<String, MongoV3StreamOffset> offset;

    public void setOffset(Map<String, MongoV3StreamOffset> offset) {
        this.offset = offset;
    }
}
