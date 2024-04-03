package io.tapdata.connector.tidb.cdc;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import org.tikv.kvproto.Cdcpb.Event.Row;

import java.io.Serializable;

public interface TiKVChangeEventDeserializationSchema<T> extends Serializable,ResultTypeQueryable<T> {
    void deserialize(Row rowRecord, Collector<T> out) throws Exception;
}
