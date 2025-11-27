package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.connector.tidb.cdc.process.TiData;
import io.tapdata.connector.tidb.cdc.process.analyse.AnalyseTapEventFromDDLObject;
import io.tapdata.connector.tidb.cdc.process.analyse.AnalyseTapEventFromDMLObject;
import io.tapdata.connector.tidb.cdc.process.ddl.entity.DDLObject;
import io.tapdata.connector.tidb.cdc.process.dml.entity.DMLObject;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

class TapEventManager {
    final AnalyseTapEventFromDMLObject dmlEventParser;
    final AnalyseTapEventFromDDLObject ddlEventParser;
    final ProcessHandler handler;
    final AtomicReference<Throwable> throwableCollector;
    final StreamReadOneByOneConsumer consumer;
    final Log log;
    final KVReadOnlyMap<TapTable> tableMap;
    Map<String, Map<String, Long>> offset;


    protected TapEventManager(ProcessHandler handler, StreamReadOneByOneConsumer consumer) {
        this.handler = handler;
        this.throwableCollector = handler.processInfo.throwableCollector;
        this.consumer = consumer;
        this.log = handler.processInfo.nodeContext.getLog();
        this.dmlEventParser = new AnalyseTapEventFromDMLObject();
        this.ddlEventParser = new AnalyseTapEventFromDDLObject(handler.processInfo.nodeContext.getTableMap());
        this.offset = parseOffset(handler.processInfo.cdcOffset);
        this.tableMap = handler.processInfo.nodeContext.getTableMap();
    }

    protected Map<String, Map<String, Long>> parseOffset(Object cdcOffset) {
        if (null == cdcOffset) return new HashMap<>();
        if (cdcOffset instanceof Map) {
            return (Map<String, Map<String, Long>>) cdcOffset;
        }
        return new HashMap<>();
    }

    public void emit(TiData tiData) {
        if (tiData instanceof DMLObject) {
            handleDML((DMLObject) tiData);
        } else if (tiData instanceof DDLObject) {
            handleDDL((DDLObject) tiData);
        } else {
            log.warn("Unrecognized incremental events: {}, neither DDL nor DML", TapSimplify.toJson(tiData));
        }
    }

    protected void handleDDL(DDLObject ddlObject) {
        Long tableVersion = ddlObject.getTableVersion();
        String table = ddlObject.getTable();
        Long lastTableVersion = TiOffset.getVersion(offset, table);
        if (null != lastTableVersion && lastTableVersion.compareTo(tableVersion) >= 0) {
            log.debug("Skip a history ddl event, table: {}, table version: {}, last version: {}, data: {}",
                table,
                tableVersion,
                lastTableVersion,
                TapSimplify.toJson(ddlObject));
            return;
        }
        TiOffset.updateVersion(offset, table, tableVersion);
        this.ddlEventParser.withConsumer(consumer, offset);
        this.ddlEventParser.analyse(ddlObject, null, log);
    }

    protected void handleDML(DMLObject dmlObject) {
        Long es = Activity.getTOSTime(dmlObject.getEs());
        Long tableVersion = dmlObject.getTableVersion();
        String table = dmlObject.getTable();
        Map<String, Long> offsetMap = TiOffset.computeIfAbsent(table, offset, es, tableVersion);
        Long lastEs = TiOffset.getEs(offsetMap);
        if (null != lastEs && lastEs.compareTo(es) > 0) {
            log.warn("Skip a history dml event, table: {}, cdc offset: {}, last offset: {}, data: {}",
                table,
                es,
                lastEs,
                TapSimplify.toJson(dmlObject));
            return;
        }
        List<TapEvent> tapRecordEvents = dmlEventParser.analyse(dmlObject, null, log);
        if (CollectionUtils.isNotEmpty(tapRecordEvents)) {
            TiOffset.updateNotNull(offset, table, es, tableVersion);
            tapRecordEvents.forEach(event -> consumer.accept(event, this.offset));
        }
    }

    public static class TiOffset {
        public static final String KEY_ES = "offset";
        public static final String KEY_TABLE_VERSION = "tableVersion";

        private TiOffset() {

        }

        public static Map<String, Long> computeIfAbsent(String table, Map<String, Map<String, Long>> offset, Long ts, Long version) {
            return offset.computeIfAbsent(table, key -> {
                Map<String, Long> map = new HashMap<>();
                map.put(KEY_ES, ts);
                map.put(KEY_TABLE_VERSION, version);
                return map;
            });
        }

        public static Long getEs(Map<String, Long> offsetMap) {
            return offsetMap.get(KEY_ES);
        }

        public static Long getVersion(Map<String, Map<String, Long>> offset, String table) {
            Map<String, Long> tableOffset = Optional.ofNullable(offset.get(table)).orElse(new HashMap<>());
            return tableOffset.get(KEY_TABLE_VERSION);
        }

        public static void updateVersion(Map<String, Map<String, Long>> offset, String table, Long tableVersion) {
            offset.compute(table, (k, v) -> {
                if (null == v) {
                    v = new HashMap<>();
                }
                v.put(KEY_TABLE_VERSION, tableVersion);
                return v;
            });
        }

        public static void updateNotNull(Map<String, Map<String, Long>> tableOffset, String table, Long offset, Long tableVersion) {
            tableOffset.compute(table, (k, v) -> {
                if (null == v) {
                    v = new HashMap<>();
                }
                if (null != offset) v.put(KEY_ES, offset);
                if (null != tableVersion) v.put(KEY_TABLE_VERSION, tableVersion);
                return v;
            });
        }

        public static Long getMinOffset(Object offset) {
            if (null == offset) return null;
            if (offset instanceof Number) return ((Number) offset).longValue();
            if (offset instanceof Map) {
                try {
                    Map<String, Map<String, Long>> o = (Map<String, Map<String, Long>>) offset;
                    long min = Activity.getTOSTime();
                    List<String> table = new ArrayList<>(o.keySet());
                    for (String tab : table) {
                        Map<String, Long> info = o.get(tab);
                        Long ts = info.get(KEY_ES);
                        if (null != ts) {
                            min = Math.min(min, ts);
                        }
                    }
                    return min;
                } catch (Exception e) {
                    return null;
                }
            }
            return null;
        }
    }
}
