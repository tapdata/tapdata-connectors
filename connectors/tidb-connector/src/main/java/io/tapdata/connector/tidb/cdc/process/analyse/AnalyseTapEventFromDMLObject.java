package io.tapdata.connector.tidb.cdc.process.analyse;

import io.tapdata.connector.tidb.cdc.process.ddl.convert.Convert;
import io.tapdata.connector.tidb.cdc.process.dml.entity.DMLObject;
import io.tapdata.connector.tidb.cdc.process.dml.entity.DMLType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnalyseTapEventFromDMLObject implements AnalyseRecord<DMLObject, List<TapEvent>> {
    public static final DefaultConvert DEFAULT_CONVERT = new DefaultConvert();

    @Override
    public List<TapEvent> analyse(DMLObject dmlObject, AnalyseColumnFilter<DMLObject> filter, Log log) {
        DMLType type = DMLType.parse(dmlObject.getType());
        switch (type) {
            case INSERT:
                return getInsertEvent(dmlObject, filter);
            case UPDATE:
                return getUpdateEvent(dmlObject, filter);
            case DELETE:
                return getDeleteEvent(dmlObject, filter);
            default:
                log.warn("Un know dml type: {}, dml info: {}", dmlObject.getType(), TapSimplify.toJson(dmlObject));
        }
        return new ArrayList<>();
    }

    protected List<TapEvent> getInsertEvent(DMLObject dmlObject, AnalyseColumnFilter<DMLObject> filter) {
        List<TapEvent> events = new ArrayList<>();
        List<Map<String, Object>> data = dmlObject.getData();
        if (CollectionUtils.isEmpty(data)) return events;
        data.forEach(eData -> {
            Map<String, Object> afterData = parseData(eData, dmlObject.getTableColumnInfo());
            if (null != filter) {
                filter.filter(null, afterData, dmlObject);
            }
            events.add(TapSimplify.insertRecordEvent(afterData, dmlObject.getTable())
                    .referenceTime(dmlObject.getEs()));
        });
        return events;
    }

    protected List<TapEvent> getUpdateEvent(DMLObject dmlObject, AnalyseColumnFilter<DMLObject> filter) {
        List<TapEvent> events = new ArrayList<>();
        List<Map<String, Object>> old = dmlObject.getOld();
        List<Map<String, Object>> data = dmlObject.getData();
        Map<String, Convert> columnInfo = dmlObject.getTableColumnInfo();
        if (CollectionUtils.isEmpty(data)) return events;
        for (int index = 0; index < data.size(); index++) {
            Map<String, Object> afterData = parseData(data.get(index), columnInfo);
            Map<String, Object> beforeData = parseData(old.get(index), columnInfo);
            if (null != filter) {
                filter.filter(beforeData, afterData, dmlObject);
            }
            events.add(TapSimplify.updateDMLEvent(beforeData, afterData, dmlObject.getTable())
                    .referenceTime(dmlObject.getEs()));
        }
        return events;
    }

    //delete data in after data list
    protected List<TapEvent> getDeleteEvent(DMLObject dmlObject, AnalyseColumnFilter<DMLObject> filter) {
        List<TapEvent> events = new ArrayList<>();
        List<Map<String, Object>> data = dmlObject.getData();
        if (CollectionUtils.isEmpty(data)) return events;
        data.forEach(eData -> {
            Map<String, Object> afterData = parseData(eData, dmlObject.getTableColumnInfo());
            if (null != filter) {
                filter.filter(null, afterData, dmlObject);
            }
            events.add(TapSimplify.deleteDMLEvent(afterData, dmlObject.getTable())
                    .referenceTime(dmlObject.getEs()));
        });
        return events;
    }

    protected Map<String, Object> parseData(Map<String, Object> data, Map<String, Convert> mysqlType) {
        if (MapUtils.isEmpty(data)) return new HashMap<>();
        if (MapUtils.isEmpty(mysqlType)) mysqlType = new HashMap<>();
        List<String> columns = new ArrayList<>(data.keySet());
        for (String column : columns) {
            Object value = data.get(column);
            if (null == value) continue;
            Convert convert = mysqlType.get(column);
            if (null == convert) continue;
            data.put(column, DEFAULT_CONVERT.convert(value, convert));
        }
        return data;
    }
}
