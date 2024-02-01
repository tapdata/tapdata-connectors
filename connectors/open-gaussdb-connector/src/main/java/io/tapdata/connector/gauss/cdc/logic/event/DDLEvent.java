package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.entity.event.TapEvent;

/**
 * @deprecated HuaWei logic replication not support ddl
 * */
public interface DDLEvent<T> extends Event<TapEvent> {

}
