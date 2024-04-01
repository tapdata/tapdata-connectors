package io.tapdata.connector.tidb.cdc;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.tikv.common.codec.TableCodec.decodeObjects;

public class TiKVChangeEventDeserializationSchemaImpl implements TiKVChangeEventDeserializationSchema, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TiKVChangeEventDeserializationSchemaImpl.class);

    private TiTableInfo tableInfo;

    private String database;

    private String tapContextId;

   private static Map<String,LinkedBlockingQueue>  logMap;

   private String tableName;






    public TiKVChangeEventDeserializationSchemaImpl(String database, String tableName, String tapContextId, TiConfiguration tiConf,
                                                    Map<String,LinkedBlockingQueue> logMap){
        this.logMap = logMap;
        TiSession session = TiSession.create(tiConf);
        this.tableName = tableName;
        this.tableInfo = session.getCatalog().getTable(database, tableName);
        this.database = database;
        this.tapContextId = tapContextId;
    }
    @Override
    public void deserialize(Cdcpb.Event.Row record, Collector out) throws Exception {
        handleRowEvent(record);
    }

    @Override
    public TypeInformation getProducedType() {
        return STRING_TYPE_INFO;
    }


    public void handleRowEvent(Cdcpb.Event.Row row) throws InterruptedException {
        LOGGER.info("11111");
        TapRecordEvent tapRecordEvent = null;
        final RowKey rowKey = RowKey.decode(row.getKey().toByteArray());
        final long handle = rowKey.getHandle();
        Object[] tikvValues;
        Map<String, Object> before;
        Map<String, Object> after;
        long eventTime = row.getCommitTs() >> 18;
        switch (row.getOpType()) {
            case PUT:
                tikvValues =
                        decodeObjects(
                                row.getValue().toByteArray(),
                                RowKey.decode(row.getKey().toByteArray()).getHandle(),
                                tableInfo);

                tapRecordEvent = new TapInsertRecordEvent().init();
                after = convert2Map(tableInfo, tikvValues);
                ((TapInsertRecordEvent) tapRecordEvent).setAfter(after);
                if (row.getOldValue().size() > 0) {
                    Object[] tikvBeforeValues =
                            decodeObjects(
                                    row.getOldValue().toByteArray(),
                                    RowKey.decode(row.getKey().toByteArray()).getHandle(),
                                    tableInfo);
                    tapRecordEvent = new TapUpdateRecordEvent().init();
                    before = convert2Map(tableInfo, tikvBeforeValues);
                    ((TapUpdateRecordEvent) tapRecordEvent).setBefore(before);
                    ((TapUpdateRecordEvent) tapRecordEvent).setAfter(after);
                }
                break;
            case DELETE:
                tikvValues = decodeObjects(row.getOldValue().toByteArray(), handle, tableInfo);
                tapRecordEvent = new TapDeleteRecordEvent().init();
                before = convert2Map(tableInfo, tikvValues);
                ((TapDeleteRecordEvent) tapRecordEvent).setBefore(before);
                break;
            default:
                break;
        }

        tapRecordEvent.setTableId(tableName);
        tapRecordEvent.setReferenceTime(eventTime);
        long offset = row.getCommitTs() >> 18;
        TidbStreamEvent tidbStreamEvent = new TidbStreamEvent(tapRecordEvent, offset);
        while (!logMap.get(database + tapContextId).offer(tidbStreamEvent, 1, TimeUnit.SECONDS)) {

        }
    }


    private Map<String, Object> convert2Map(TiTableInfo tableInfo, Object[] tikvValues) {
        if (null == tikvValues) return Collections.emptyMap();
        Map<String, Object> result = new HashMap<>();
        List<TiColumnInfo> tiColumnInfos = tableInfo.getColumns();
        if (CollectionUtils.isEmpty(tiColumnInfos)) return Collections.emptyMap();
        for (int index = 0; index < tiColumnInfos.size(); index++) {
            String fieldName = tiColumnInfos.get(index).getName();
            Object value = tikvValues[index];
            result.put(fieldName, value);
        }
        return result;
    }

}
