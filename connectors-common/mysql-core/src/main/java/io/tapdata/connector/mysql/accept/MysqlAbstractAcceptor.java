package io.tapdata.connector.mysql.accept;

import io.tapdata.cdc.Acceptor;
import io.tapdata.connector.mysql.entity.MysqlStreamEvent;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/16 09:43 Create
 * @description
 */
public abstract class MysqlAbstractAcceptor<M, C extends TapStreamReadConsumer<?, Object>> implements Acceptor<M, MysqlStreamEvent, C> {
    protected Object offset;

    public void updateOffset(Object offset) {
        this.offset = offset;
    }

    public Object getOffset() {
        return offset;
    }

    public void complete() {
        //do nothing
    }
}
