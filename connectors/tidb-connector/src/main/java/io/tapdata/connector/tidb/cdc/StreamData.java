package io.tapdata.connector.tidb.cdc;

import io.tapdata.entity.logger.Log;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.tikv.common.TiConfiguration;
import org.tikv.kvproto.Kvrpcpb;

import java.io.Serializable;

public class StreamData implements Serializable{


    public JobClient startStream(String database, String tableName, TiConfiguration tiConf, long startTs,
                                 TiKVChangeEventDeserializationSchema tiKVChangeEventDeserializationSchema, Log tapLogger) {
        JobClient jobClient = null;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        try {
            if(database == null || tableName ==null){
                throw new Exception("StartStream error database or tableName is null");
            }
            SourceFunction<String> tidbSource =
                    TidbSource.<String>builder()
                            .database(database)
                            .tableName(tableName)
                            .tiConf(tiConf)
                            .snapshotEventDeserializer(
                                    new TiKVSnapshotEventDeserializationSchema<String>() {
                                        @Override
                                        public void deserialize(
                                                Kvrpcpb.KvPair data, Collector<String> out)
                                                throws Exception {
                                            out.collect(data.toString());
                                        }

                                        @Override
                                        public TypeInformation<String> getProducedType() {
                                            return BasicTypeInfo.STRING_TYPE_INFO;
                                        }
                                    })
                            .changeEventDeserializer(tiKVChangeEventDeserializationSchema)
                            .startTs(startTs)
                            .build();
            env.enableCheckpointing(3000);
            env.addSource(tidbSource).print().setParallelism(1);
            jobClient = env.executeAsync(database + tableName);
        } catch (Exception e) {
            tapLogger.error(" start tidb stream error:{}",e.getMessage());
        }
        return jobClient;
    }

}
