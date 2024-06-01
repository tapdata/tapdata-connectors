package io.tapdata.connector.tidb.cdc.process.analyse;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.simplify.TapSimplify;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.fromJson;
import static io.tapdata.connector.tidb.cdc.process.analyse.TiDBDataTypeConvert.DELETE;
import static io.tapdata.connector.tidb.cdc.process.analyse.TiDBDataTypeConvert.INSERT;

/**
 * @author GavinXiao
 * @description AnalyseTapEventFromCsvString create by Gavin
 * @create 2023/7/14 10:00
 **/
public class AnalyseTapEventFromCsvString implements AnalyseRecord<String[], TapRecordEvent> {
    public static final DefaultConvert DEFAULT_CONVERT = new DefaultConvert();

    /**
     * @param tapTable {
     *                 "columnName": "sybaseType"
     *                 ....
     *                 }
     */
    @Override
    public TapRecordEvent analyse(String[] record, LinkedHashMap<String, TableTypeEntity> tapTable, String tableId, CsvAnalyseFilter filter) {
        final int recordKeyCount = record.length;
        final int group = recordKeyCount / 3;

        final int fieldsCount = tapTable.size();

        final int cdcTypeIndex = (group - 1) * 3;
        final String cdcType = fieldsCount < group ? record[cdcTypeIndex] : INSERT;
        String cdcInfoStr = fieldsCount < group ? record[cdcTypeIndex + 1] : null;
        Map<String, Object> cdcInfo = null;
        try {
            cdcInfo = (Map<String, Object>) fromJson(cdcInfoStr);
        } catch (Exception e) {
            cdcInfo = new HashMap<>();
        }
        if (null == cdcInfo) cdcInfo = new HashMap<>();

        int index = 0;
        Map<String, Object> after = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        final boolean isDel = DELETE.equals(cdcType);
        final boolean isIns = INSERT.equals(cdcType);
        for (Map.Entry<String, TableTypeEntity> fieldEntry : tapTable.entrySet()) {
            final String fieldName = fieldEntry.getKey();
            final TableTypeEntity typeEntity = fieldEntry.getValue();
            final String sybaseType = typeEntity.getType();

            //ignore timestamp
            if (null != sybaseType && sybaseType.toUpperCase().startsWith("TIMESTAMP")) {
                index++;
                continue;
            }

            int fieldValueIndex = index * 3;
            if (!isDel) {
                final Object value = recordKeyCount <= fieldValueIndex ? null : record[fieldValueIndex];
                after.put(fieldName, DEFAULT_CONVERT.convert(value, sybaseType, typeEntity.getTypeNum()));
            }
            if (!isIns) {
                int fieldBeforeValueIndex = fieldValueIndex + 1;
                final Object beforeValue = recordKeyCount <= fieldBeforeValueIndex ? null : record[fieldBeforeValueIndex];
                before.put(fieldName, DEFAULT_CONVERT.convert(beforeValue, sybaseType, typeEntity.getTypeNum()));
            }
            index++;
        }

        if (null != filter) {
            filter.filter(before, after, cdcInfo);
        }

        Object timestamp = cdcInfo.get("timestamp");
        long cdcReference = System.currentTimeMillis();
        try {
            cdcReference = timestamp instanceof Number ? ((Number)timestamp).longValue() : Long.parseLong(String.valueOf(timestamp));
        } catch (Exception ignore) { }
        switch (cdcType) {
            case INSERT:
                return TapSimplify.insertRecordEvent(after, tableId).referenceTime(cdcReference);
            case DELETE:
                return TapSimplify.deleteDMLEvent(before, tableId).referenceTime(cdcReference);
            default:
                return TapSimplify.updateDMLEvent(before, after, tableId).referenceTime(cdcReference);
        }
    }
}
