package io.tapdata.oceanbase.cdc;

import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.netty.util.BooleanSupplier;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.connector.mysql.ddl.ccj.MysqlDDLWrapper;
import io.tapdata.data.ob.*;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.util.DateUtil;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.list;

public class OceanbaseReaderV2 {

    private OceanbaseConfig oceanbaseConfig;
    private List<String> tableList;
    private KVReadOnlyMap<TapTable> tableMap;
    private StreamReadConsumer consumer;
    private Object offsetState;
    private int recordSize;
    private final Map<String, String> dataTypeMap = new HashMap<>();
    private final Map<String, String> dataFormatMap = new HashMap<>();
    private final String connectorId;
    private ObReadLogServerGrpc.ObReadLogServerBlockingStub blockingStub;
    protected final AtomicBoolean ddlStop = new AtomicBoolean(false);
    private DDLParserType ddlParserType = DDLParserType.MYSQL_CCJ_SQL_PARSER;
    private static final DDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("`");


    public OceanbaseReaderV2(OceanbaseConfig oceanbaseConfig, String connectorId) {
        this.oceanbaseConfig = oceanbaseConfig;
        this.connectorId = connectorId;
    }

    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        this.tableList = tableList;
        this.offsetState = offsetState;
        this.recordSize = recordSize;
        this.consumer = consumer;
        this.tableMap = tableMap;
        generateDataTypeMap(tableMap);
        blockingStub = ObReadLogServerGrpc.newBlockingStub(NettyChannelBuilder.forAddress(oceanbaseConfig.getRawLogServerHost(), oceanbaseConfig.getRawLogServerPort())
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .negotiationType(NegotiationType.PLAINTEXT).build());
    }

    private void generateDataTypeMap(KVReadOnlyMap<TapTable> tableMap) {
        for (String table : tableList) {
            TapTable tapTable = tableMap.get(table);
            if (tapTable != null) {
                for (Map.Entry<String, TapField> entry : tapTable.getNameFieldMap().entrySet()) {
                    String dataType = entry.getValue().getDataType();
                    dataTypeMap.put(table + "." + entry.getKey(), StringKit.removeParentheses(dataType));
                }
            }
        }
    }

    public void start(BooleanSupplier isAlive) throws Throwable {
        Iterator<ReadLogResponse> iterator = blockingStub.startReadLog(ReadLogRequest.newBuilder().setSource(
                ReaderSource.newBuilder()
                        .setRootserverList(oceanbaseConfig.getRootServerList())
                        .setClusterUser(oceanbaseConfig.getCdcUser())
                        .setClusterPassword(oceanbaseConfig.getCdcPassword())
                        .setTbWhiteList(tableList.stream().map(table -> oceanbaseConfig.getTenant() + "." + oceanbaseConfig.getDatabase() + "." + table).collect(Collectors.joining("|")))
                        .build()).setStime((Long) offsetState).setTaskId(connectorId).build());
        consumer.streamReadStarted();
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        try (
                ConcurrentProcessor<ReadLogPayload, MessageEvent> concurrentProcessor = TapExecutors.createSimple(8, 32, "OceanBaseReader-Processor")
        ) {
            Thread t = new Thread(() -> {
                AtomicReference<List<TapEvent>> events = new AtomicReference<>(list());
                int heartbeat = 0;
                long lastTimestamp = 0L;
                try {
                    while (isAlive.get()) {
                        MessageEvent messageEvent = concurrentProcessor.get(100, TimeUnit.MILLISECONDS);
                        if (EmptyKit.isNotNull(messageEvent)) {
                            if (messageEvent.getPayload().getOpValue() == 7) {
                                if (heartbeat++ > 3) {
                                    consumer.accept(Collections.singletonList(new HeartbeatEvent().init().referenceTime(messageEvent.getPayload().getTransactionTime() * 1000)), messageEvent.getPayload().getTransactionTime());
                                    heartbeat = 0;
                                }
                                continue;
                            }
                            events.get().addAll(messageEvent.getEvent());
                            lastTimestamp = messageEvent.getPayload().getTransactionTime();
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
            while (isAlive.get() && iterator.hasNext()) {
                ReadLogPayload payload = iterator.next().getPayload();
                while (ddlStop.get() && isAlive.get()) {
                    TapSimplify.sleep(500);
                }
                if (payload.getOpValue() == 6) {
                    ddlStop.set(true);
                }
                try {
                    concurrentProcessor.runAsync(payload, e -> {
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
        }
        consumer.streamReadEnded();
        if (EmptyKit.isNotNull(throwable.get())) {
            throw throwable.get();
        }
        if (isAlive.get()) {
            throw new RuntimeException("Exception occurs in OceanBase Log Miner service");
        }
    }

    private MessageEvent emit(ReadLogPayload payload) throws UnsupportedEncodingException {
        String tableName = payload.getTbname();
        AtomicLong lastTransactionTime = new AtomicLong(payload.getTransactionTime());
        List<TapEvent> tapEvents = new ArrayList<>();
        switch (payload.getOpValue()) {
            case 3:
                tapEvents.add(new TapInsertRecordEvent().init().table(tableName)
                        .after(getRecord(payload, false)).referenceTime(lastTransactionTime.get() * 1000));
                break;
            case 4:
                tapEvents.add(new TapUpdateRecordEvent().init().table(tableName)
                        .after(getRecord(payload, false)).before(getRecord(payload, true))
                        .referenceTime(lastTransactionTime.get() * 1000));
                break;
            case 5:
                tapEvents.add(new TapDeleteRecordEvent().init().table(tableName)
                        .before(getRecord(payload, true)).referenceTime(lastTransactionTime.get() * 1000));
                break;
            case 6:
                try {
                    DDLFactory.ddlToTapDDLEvent(ddlParserType, payload.getDdl(),
                            DDL_WRAPPER_CONFIG,
                            tableMap,
                            tapDDLEvent -> {
                                tapDDLEvent.setTime(System.currentTimeMillis());
                                tapDDLEvent.setReferenceTime(lastTransactionTime.get() * 1000);
                                tapDDLEvent.setOriginDDL(payload.getDdl());
                                tapEvents.add(tapDDLEvent);
                            }, (ddl, wrapper) -> {
                                boolean unIgnoreTable = true;
                                if (wrapper instanceof MysqlDDLWrapper) {
                                    String table = ((MysqlDDLWrapper) wrapper).getTableName(ddl);
                                    unIgnoreTable = null == tableList || tableList.contains(table);
                                }
                                return unIgnoreTable;
                            });
                    generateDataTypeMap(tableMap);
                } catch (Throwable e) {
                    TapDDLEvent tapDDLEvent = new TapDDLUnknownEvent();
                    tapDDLEvent.setTime(System.currentTimeMillis());
                    tapDDLEvent.setReferenceTime(lastTransactionTime.get() * 1000);
                    tapDDLEvent.setOriginDDL(payload.getDdl());
                    tapEvents.add(tapDDLEvent);
                } finally {
                    ddlStop.set(false);
                }
                break;
            case 7:
                tapEvents.add(new HeartbeatEvent().init().referenceTime(lastTransactionTime.get() * 1000));
                break;
            default:
                throw new RuntimeException("Unknown operation type: " + payload.getOpValue());
        }
        return new MessageEvent(tapEvents, payload);
    }

    public void stop() {
        if (blockingStub != null) {
            blockingStub.stopReadLog(StopReadLogRequest.newBuilder().setTaskId(connectorId).build());
        }
    }

    private Map<String, Object> getRecord(ReadLogPayload payload, boolean isBefore) throws UnsupportedEncodingException {
        Map<String, Object> record = new HashMap<>();
        if (isBefore) {
            for (int i = 0; i < payload.getBeforeCount(); i++) {
                String columnName = payload.getBefore(i).getColumnName();
                record.put(columnName, parseValueString(payload.getTbname() + "." + columnName, payload.getBefore(i)));
            }
        } else {
            for (int i = 0; i < payload.getAfterCount(); i++) {
                String columnName = payload.getAfter(i).getColumnName();
                record.put(columnName, parseValueString(payload.getTbname() + "." + columnName, payload.getAfter(i)));
            }
        }
        return record;
    }

    private Object parseValueString(String column, Value value) throws UnsupportedEncodingException {
        String dataType = dataTypeMap.get(column);
        if (value.getIsNull()) {
            return null;
        }
        String valueString = value.getValueString();
        switch (dataType) {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
            case "bigint":
            case "decimal":
            case "double":
            case "float":
                return new BigDecimal(valueString);
            case "datetime": {
                String dataFormat = dataFormatMap.get(column);
                if (EmptyKit.isNull(dataFormat)) {
                    dataFormat = DateUtil.determineDateFormat(valueString);
                    dataFormatMap.put(column, dataFormat);
                }
                return DateUtil.parseInstantWithHour(valueString, dataFormat, oceanbaseConfig.getZoneOffsetHour());
            }
            case "date": {
                return LocalDate.parse(valueString).atStartOfDay();
            }
            case "timestamp": {
                String dataFormat = dataFormatMap.get(column);
                if (EmptyKit.isNull(dataFormat)) {
                    dataFormat = DateUtil.determineDateFormat(valueString);
                    dataFormatMap.put(column, dataFormat);
                }
                return DateUtil.parseInstantWithZone(valueString, dataFormat, ZoneOffset.UTC);
            }
            case "year":
                return Integer.parseInt(valueString);
            case "bit":
                if (valueString.length() > 1) {
                    return valueString.getBytes();
                } else {
                    return "1".equals(valueString);
                }
            case "binary":
            case "varbinary":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                return valueString.getBytes();
            default:
                return valueString;
        }
    }

    static class MessageEvent {
        private final ReadLogPayload payload;
        private final List<TapEvent> event;

        public MessageEvent(List<TapEvent> event, ReadLogPayload payload) {
            this.payload = payload;
            this.event = event;
        }

        public ReadLogPayload getPayload() {
            return payload;
        }

        public List<TapEvent> getEvent() {
            return event;
        }
    }

}
