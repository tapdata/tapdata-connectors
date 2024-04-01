package io.tapdata.connector.tidb.cdc;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import org.tikv.kvproto.Kvrpcpb.KvPair;

import java.io.Serializable;

public interface TiKVSnapshotEventDeserializationSchema <T> extends Serializable ,ResultTypeQueryable<T> {
    void deserialize(KvPair record, Collector<T> out) throws Exception;

}
