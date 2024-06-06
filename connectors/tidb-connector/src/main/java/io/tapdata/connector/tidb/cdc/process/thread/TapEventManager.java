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
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

class TapEventManager implements Activity {
    final AnalyseTapEventFromDMLObject dmlEventParser;
    final AnalyseTapEventFromDDLObject ddlEventParser;
    final ProcessHandler handler;
    final AtomicReference<Throwable> throwableCollector;
    final StreamReadConsumer consumer;
    final Log log;
    final KVReadOnlyMap<TapTable> tableMap;
    Map<String, Map<String, Long>> offset;


    protected TapEventManager(ProcessHandler handler, StreamReadConsumer consumer) {
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

    @Override
    public void init() {

    }

    @Override
    public void doActivity() {
        //do nothing
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
        String table = dmlObject.getTable();
        Map<String, Long> offsetMap = TiOffset.computeIfAbsent(table, offset, es, dmlObject.getTableVersion());
        Long lastEs = TiOffset.getEs(offsetMap);
        if (null != lastEs && lastEs.compareTo(es) > 0) {
            log.debug("Skip a history dml event, table: {}, cdc offset: {}, last offset: {}, data: {}",
                    table,
                    es,
                    lastEs,
                    TapSimplify.toJson(dmlObject));
            return;
        }
        List<TapEvent> tapRecordEvents = dmlEventParser.analyse(dmlObject, null, log);
        if (CollectionUtils.isNotEmpty(tapRecordEvents)) {
            consumer.accept(tapRecordEvents, this.offset);
        }
    }

    @Override
    public void close() throws Exception {
        //do nothing
    }

    public static class TiOffset {
        private TiOffset() {

        }

        public static Map<String, Long> computeIfAbsent(String table, Map<String, Map<String, Long>> offset, Long ts, Long version) {
            return offset.computeIfAbsent(table, key -> {
                Map<String, Long> map = new HashMap<>();
                map.put("offset", ts);
                map.put("tableVersion", version);
                return map;
            });
        }

        public static Long getEs(Map<String, Long> offsetMap) {
            return offsetMap.get("offset");
        }

        public static Long getVersion(Map<String, Map<String, Long>> offset, String table) {
            Map<String, Long> tableOffset = Optional.ofNullable(offset.get(table)).orElse(new HashMap<>());
            return tableOffset.get("tableVersion");
        }

        public static void updateVersion(Map<String, Map<String, Long>> offset, String table, Long tableVersion) {
            Map<String, Long> tableOffset = Optional.ofNullable(offset.get(table)).orElse(new HashMap<>());
            tableOffset.put("tableVersion", tableVersion);
            offset.put(table, tableOffset);
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
                        Long ts = info.get("offset");
                        if (null == ts) continue;
                        if (ts.compareTo(min) < 0) {
                            min = ts;
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
