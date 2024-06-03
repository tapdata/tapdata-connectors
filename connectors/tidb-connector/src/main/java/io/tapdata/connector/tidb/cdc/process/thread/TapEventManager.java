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
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class TapEventManager implements Activity {
    final AnalyseTapEventFromDMLObject dmlEventParser;
    final AnalyseTapEventFromDDLObject ddlEventParser;
    LinkedBlockingQueue<TiData> data;
    ScheduledFuture<?> scheduledFuture;
    final ProcessHandler handler;
    final AtomicReference<Throwable> throwableCollector;
    final StreamReadConsumer consumer;
    final Log log;
    final Object emitLock;
    final KVReadOnlyMap<TapTable> tableMap;
    Map<String, Map<String, Long>> offset;


    protected TapEventManager(ProcessHandler handler, int maxQueueSize, StreamReadConsumer consumer) {
        this.handler = handler;
        if (maxQueueSize < 1 || maxQueueSize > 5000) maxQueueSize = 500;
        this.data = new LinkedBlockingQueue<>(maxQueueSize);
        this.throwableCollector = handler.processInfo.throwableCollector;
        this.consumer = consumer;
        this.log = handler.processInfo.nodeContext.getLog();
        this.emitLock = new Object();
        this.dmlEventParser = new AnalyseTapEventFromDMLObject();
        this.ddlEventParser = new AnalyseTapEventFromDDLObject(handler.processInfo.nodeContext.getTableMap());
        this.tableMap = handler.processInfo.nodeContext.getTableMap();
        this.offset = parseOffset(handler.processInfo.cdcOffset);
    }

    protected Map<String, Map<String, Long>> parseOffset(Object cdcOffset) {
        if (null == cdcOffset) return new HashMap<>();
        if (cdcOffset instanceof Map) {
            return (Map<String, Map<String, Long>>) cdcOffset;
        }
        return new HashMap<>();
    }

    public void emit(Map<String, List<? extends TiData>> dataMap) {
        synchronized (this.emitLock) {
            dataMap.forEach((table, dataList) -> data.addAll(dataList));
        }
    }

    public void emit(TiData tiData) {
        synchronized (this.emitLock) {
            data.add(tiData);
        }
    }

    @Override
    public void init() {

    }

    @Override
    public void doActivity() {
        cancelSchedule(this.scheduledFuture, log);
        this.scheduledFuture = this.handler.getScheduledExecutorService().scheduleWithFixedDelay(() -> {
            try {
                while (!data.isEmpty()) {
                    TiData poll = data.poll();
                    if (poll instanceof DMLObject) {
                        handleDML((DMLObject) poll);
                    } else if (poll instanceof DDLObject) {
                        handleDDL((DDLObject) poll);
                    } else {
                        log.warn("Unrecognized incremental events: {}, neither DDL nor DML", TapSimplify.toJson(poll));
                    }
                }
            } catch (Throwable t) {
                synchronized (throwableCollector) {
                    throwableCollector.set(t);
                }
            }
        }, 2, 1, TimeUnit.SECONDS);
    }

    protected void handleDDL(DDLObject ddlObject) {
        log.debug("find a DDL: " + TapSimplify.toJson(ddlObject));
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
        TapEvent ddlResult = this.ddlEventParser.analyse(ddlObject, null);
        if (null != ddlResult) {
            ddlResult.setTime(System.currentTimeMillis());
            List<TapEvent> ddl = new ArrayList<>();
            ddl.add(ddlResult);
            consumer.accept(ddl, this.offset);
            log.debug("Accept a DDL: " + TapSimplify.toJson(ddlObject));
        }

    }

    protected void handleDML(DMLObject dmlObject) {
        log.debug("Find a DML: " + TapSimplify.toJson(dmlObject));
        Long ts = Optional.ofNullable(dmlObject.getTs()).orElse(System.currentTimeMillis());
        String table = dmlObject.getTable();
        Map<String, Long> offsetMap = TiOffset.computeIfAbsent(table, offset, ts, dmlObject.getTableVersion());
        Long lastTs = TiOffset.getTs(offsetMap);
        if (null != lastTs && lastTs.compareTo(ts) > 0) {
            log.debug("Skip a history dml event, table: {}, cdc offset: {}, last offset: {}, data: {}",
                    table,
                    ts,
                    lastTs,
                    TapSimplify.toJson(dmlObject));
            return;
        }
        List<TapEvent> tapRecordEvents = dmlEventParser.analyse(dmlObject, null);
        if (CollectionUtils.isNotEmpty(tapRecordEvents)) {
            consumer.accept(tapRecordEvents, this.offset);
            log.debug("Accept a DML: " + TapSimplify.toJson(dmlObject));
        }
    }

    @Override
    public void close() throws Exception {
        cancelSchedule(this.scheduledFuture, log);
        this.scheduledFuture = null;
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

        public static Long getTs(Map<String, Long> offsetMap) {
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
            if (offset instanceof Number) return ((Number)offset).longValue();
            if (offset instanceof Map) {
                try {
                    Map<String, Map<String, Long>> o = (Map<String, Map<String, Long>>) offset;
                    long min = System.currentTimeMillis();
                    List<String> table = new ArrayList<>(o.keySet());
                    Long logicalClock = null;
                    for (String tab : table) {
                        Map<String, Long> info = o.get(tab);
                        Long ts = info.get("offset");
                        if (null == ts) continue;
                        if (ts.compareTo(min) < 0) {
                            min = ts;
                            logicalClock = info.get("tableVersion");
                        }
                    }
                    return generateTSO(min, logicalClock);
                } catch (Exception e) {
                    return null;
                }
            }
            return null;
        }
        public static Long generateTSO(long cdcOffset, Long logicalClock) {
            if (null == logicalClock) return null;
            long timeHighBits = cdcOffset >> 32; // 高32位
            long timeLowBits = cdcOffset << 32; // 低32位
            long logicalClockBits = logicalClock & 0xFFFFFFFFL; // 逻辑时钟的低32位

            return timeHighBits | timeLowBits | logicalClockBits;
        }
    }
}
