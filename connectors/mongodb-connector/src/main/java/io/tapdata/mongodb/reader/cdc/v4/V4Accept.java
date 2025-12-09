package io.tapdata.mongodb.reader.cdc.v4;

import io.tapdata.mongodb.reader.cdc.Acceptor;
import io.tapdata.mongodb.reader.MongodbV4StreamReader;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/9 11:07 Create
 * @description
 */
public abstract class V4Accept<T extends V4Accept<T, Consumer>, Consumer extends TapStreamReadConsumer<?, Object>> implements Acceptor<T, MongodbV4StreamReader.OffsetEvent, Consumer> {
}
