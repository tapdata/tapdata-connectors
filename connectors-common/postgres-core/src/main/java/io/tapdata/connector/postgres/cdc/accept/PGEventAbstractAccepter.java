package io.tapdata.connector.postgres.cdc.accept;

import io.tapdata.cdc.EventAbstractAccepter;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/10 17:16 Create
 * @description
 */
public abstract class PGEventAbstractAccepter<T extends EventAbstractAccepter<T, Consumer>, Consumer extends TapStreamReadConsumer<?, Object>> extends EventAbstractAccepter<T, Consumer> {
    protected Map<String, ?> offset;

    public T updateOffset(Map<String, ?> offset) {
        this.offset = offset;
        return (T) this;
    }
}
