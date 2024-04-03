package io.tapdata.connector.tidb.cdc;


import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.tikv.common.TiConfiguration;

import java.io.Serializable;

public class TidbSource implements Serializable {
    public static <T> Builder<T> builder() {
        return new Builder();
    }

    public static class Builder<T> {
        private String database;
        private String tableName;
        private TiConfiguration tiConf;
        private TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema;
        private TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema;
        private long startTs;

        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }

        public Builder<T> tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder<T> snapshotEventDeserializer(TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema) {
            this.snapshotEventDeserializationSchema = snapshotEventDeserializationSchema;
            return this;
        }

        public Builder<T> changeEventDeserializer(TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema) {
            this.changeEventDeserializationSchema = changeEventDeserializationSchema;
            return this;
        }


        public Builder<T> startTs(long startTs) {
            this.startTs = startTs;
            return this;
        }

        public Builder<T> tiConf(TiConfiguration tiConf) {
            this.tiConf = tiConf;
            return this;
        }

        public RichParallelSourceFunction<T> build() {
            return new TiKVParallelSourceFunction(this.snapshotEventDeserializationSchema, this.changeEventDeserializationSchema, this.tiConf, this.database, this.tableName, this.startTs);
        }
    }
}
