package io.tapdata.oceanbase.cdc;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import io.netty.util.BooleanSupplier;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.connector.mysql.ddl.ccj.MysqlDDLWrapper;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.base.ConnectorBase.list;

public class OceanbaseReader {

    private OceanbaseConfig oceanbaseConfig;
    private KVReadOnlyMap<TapTable> tableMap;
    private List<String> tableList;
    private StreamReadConsumer consumer;
    private Object offsetState;
    private int recordSize;
    private final Map<String, String> dataFormatMap = new HashMap<>();
    private DDLParserType ddlParserType = DDLParserType.MYSQL_CCJ_SQL_PARSER;
    private static final DDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("`");

    public OceanbaseReader(OceanbaseConfig oceanbaseConfig) {
        this.oceanbaseConfig = oceanbaseConfig;
    }

    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        this.tableList = tableList;
        this.tableMap = tableMap;
        this.offsetState = offsetState;
        this.recordSize = recordSize;
        this.consumer = consumer;
    }

    public void start(BooleanSupplier isAlive) throws Throwable {
        ObReaderConfig config = new ObReaderConfig();
        config.setRsList(oceanbaseConfig.getHost() + ":" + oceanbaseConfig.getRpcPort() + ":" + oceanbaseConfig.getPort());
        config.setUsername(oceanbaseConfig.getUser());
        config.setPassword(oceanbaseConfig.getPassword());
        config.setStartTimestamp((Long) offsetState);
        config.setTableWhiteList(oceanbaseConfig.getTenant() + "." + oceanbaseConfig.getDatabase() + ".*");
        LogProxyClient client = new LogProxyClient(oceanbaseConfig.getHost(), oceanbaseConfig.getLogProxyPort(), config);
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        try (
                ConcurrentProcessor<LogMessage, MessageEvent> concurrentProcessor = TapExecutors.createSimple(8, 32, "OceanBaseReader-Processor")
        ) {
            Thread t = new Thread(() -> {
                AtomicReference<List<TapEvent>> events = new AtomicReference<>(list());
                int heartbeat = 0;
                long lastTimestamp = 0L;
                try {
                    while (isAlive.get()) {
                        MessageEvent messageEvent = concurrentProcessor.get(100, TimeUnit.MILLISECONDS);
                        if (EmptyKit.isNotNull(messageEvent)) {
                            if (EmptyKit.isNull(messageEvent.getEvent())) {
                                continue;
                            }
                            if ("HEARTBEAT".equals(messageEvent.getMessage().getOpt().name())) {
                                if (heartbeat++ > 3) {
                                    consumer.accept(Collections.singletonList(new HeartbeatEvent().init().referenceTime(Long.parseLong(messageEvent.getMessage().getTimestamp()) * 1000)), Long.parseLong(messageEvent.getMessage().getTimestamp()));
                                    heartbeat = 0;
                                }
                                continue;
                            }
                            events.get().add(messageEvent.getEvent());
                            lastTimestamp = Long.parseLong(messageEvent.getMessage().getTimestamp());
                            if (events.get().size() >= recordSize) {
                                consumer.accept(events.get(), lastTimestamp);
                                events.set(new ArrayList<>());
                            }
                        } else {
                            if (events.get().size() > 0) {
                                consumer.accept(events.get(), lastTimestamp);
                                events.set(new ArrayList<>());
                            }
                        }
                    }
                } catch (Exception e) {
                    throwable.set(e);
                }
            });
            t.setName("OceanBaseReader-Consumer");
            t.start();
            client.addListener(new RecordListener() {
                @Override
                public void notify(LogMessage message) {
                    try {
                        concurrentProcessor.runAsync(message, e -> {
                            try {
                                return emit(e);
                            } catch (Exception er) {
                                throwable.set(er);
                                return null;
                            }
                        });
                    } catch (Exception e) {
                        throwable.set(e);
                    }
                }

                @Override
                public void onException(LogProxyClientException e) {
                    if (e.needStop()) {
                        client.stop();
                    }
                }
            });
            client.start();
            consumer.streamReadStarted();
            client.join();
        }
        consumer.streamReadEnded();
        if (EmptyKit.isNotNull(throwable.get())) {
            throw throwable.get();
        }
        if (isAlive.get()) {
            throw new RuntimeException("Exception occurs in OceanBase Log Miner service");
        }
    }

    private MessageEvent emit(LogMessage message) {
        String op = message.getOpt().name();
        if (!tableList.contains(message.getTableName())) {
            switch (op) {
                case "INSERT":
                case "UPDATE":
                case "DELETE":
                    return null;
            }
        }
        AtomicReference<TapEvent> tapEvent = new AtomicReference<>();
        Map<String, Object> after = DataMap.create();
        Map<String, Object> before = DataMap.create();
        analyzeMessage(message, after, before);
        switch (op) {
            case "INSERT":
                tapEvent.set(new TapInsertRecordEvent().init().table(message.getTableName()).after(after).referenceTime(Long.parseLong(message.getTimestamp()) * 1000));
                break;
            case "UPDATE":
                tapEvent.set(new TapUpdateRecordEvent().init().table(message.getTableName()).after(after).before(before).referenceTime(Long.parseLong(message.getTimestamp()) * 1000));
                break;
            case "DELETE":
                tapEvent.set(new TapDeleteRecordEvent().init().table(message.getTableName()).before(before).referenceTime(Long.parseLong(message.getTimestamp()) * 1000));
                break;
            case "HEARTBEAT":
                tapEvent.set(new HeartbeatEvent().init().referenceTime(Long.parseLong(message.getTimestamp()) * 1000));
                break;
            case "DDL": {
                String ddlStr = message.getFieldList().get(0).getValue().toString();
                if (StringUtils.isNotBlank(ddlStr)) {
                    try {
                        DDLFactory.ddlToTapDDLEvent(
                                ddlParserType,
                                ddlStr,
                                DDL_WRAPPER_CONFIG,
                                tableMap,
                                tapDDLEvent -> {
                                    tapDDLEvent.setTime(System.currentTimeMillis());
                                    tapDDLEvent.setReferenceTime(Long.parseLong(message.getTimestamp()) * 1000);
                                    tapDDLEvent.setOriginDDL(ddlStr);
                                    tapEvent.set(tapDDLEvent);
                                }, (ddl, wrapper) -> {
                                    boolean unIgnoreTable = true;
                                    if (wrapper instanceof MysqlDDLWrapper) {
                                        String tableName = ((MysqlDDLWrapper) wrapper).getTableName(ddl);
                                        unIgnoreTable = null == tableList || tableList.contains(tableName);
                                    }
                                    return unIgnoreTable;
                                }
                        );
                    } catch (Throwable e) {
                        TapDDLEvent tapDDLEvent = new TapDDLUnknownEvent();
                        tapDDLEvent.setTime(System.currentTimeMillis());
                        tapDDLEvent.setReferenceTime(Long.parseLong(message.getTimestamp()) * 1000);
                        tapDDLEvent.setOriginDDL(ddlStr);
                        tapEvent.set(tapDDLEvent);
                    }
                }
                break;
            }
            default:
                return null;
        }
        return new MessageEvent(tapEvent.get(), message);
    }

    private void analyzeMessage(LogMessage message, Map<String, Object> after, Map<String, Object> before) {
        String table = message.getTableName();
        switch (message.getOpt().name()) {
            case "INSERT":
                message.getFieldList().forEach(k -> after.put(k.getFieldname(), parseField(table, k)));
                break;
            case "UPDATE": {
                int index = 0;
                for (DataMessage.Record.Field field : message.getFieldList()) {
                    if (index % 2 == 0) {
                        before.put(field.getFieldname(), parseField(table, field));
                    } else {
                        after.put(field.getFieldname(), parseField(table, field));
                    }
                    index++;
                }
                break;
            }
            case "DELETE":
                message.getFieldList().forEach(k -> before.put(k.getFieldname(), parseField(table, k)));
                break;
            default:
                break;
        }
    }

    private Object parseField(String table, DataMessage.Record.Field field) {
        if (EmptyKit.isNull(field.getValue())) {
            return null;
        }
        switch (field.getType().name()) {
            case "INT8":
            case "INT16":
            case "INT24":
            case "INT32":
            case "INT64":
            case "DECIMAL":
            case "DOUBLE":
            case "FLOAT":
                return new BigDecimal(field.getValue().toString());
            case "DATETIME": {
                String dataFormat = dataFormatMap.get(table + "." + field.getFieldname());
                if (EmptyKit.isNull(dataFormat)) {
                    dataFormat = DateUtil.determineDateFormat(field.getValue().toString());
                    dataFormatMap.put(table + "." + field.getFieldname(), dataFormat);
                }
                return DateUtil.parseInstantWithHour(field.getValue().toString(), dataFormat, oceanbaseConfig.getZoneOffsetHour());
            }
            case "DATE": {
                return LocalDate.parse(field.getValue().toString()).atStartOfDay();
            }
            case "TIMESTAMP": {
                String dataFormat = dataFormatMap.get(table + "." + field.getFieldname());
                if (EmptyKit.isNull(dataFormat)) {
                    dataFormat = DateUtil.determineDateFormat(field.getValue().toString());
                    dataFormatMap.put(table + "." + field.getFieldname(), dataFormat);
                }
                return DateUtil.parseInstantWithZone(field.getValue().toString(), dataFormat, ZoneOffset.UTC);
            }
            case "YEAR":
                return Integer.parseInt(field.getValue().toString());
            case "BIT":
                if (field.getValue().getLen() > 1) {
                    return field.getValue().getBytes();
                } else {
                    return "1".equals(field.getValue().toString());
                }
            case "BLOB":
            case "BINARY":
                return field.getValue().getBytes();
            default:
                return field.getValue().toString();
        }
    }

    static class MessageEvent {
        private final LogMessage message;
        private final TapEvent event;

        public MessageEvent(TapEvent event, LogMessage message) {
            this.message = message;
            this.event = event;
        }

        public LogMessage getMessage() {
            return message;
        }

        public TapEvent getEvent() {
            return event;
        }
    }
}
