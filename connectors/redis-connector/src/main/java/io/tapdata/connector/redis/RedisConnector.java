package io.tapdata.connector.redis;

import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.Command;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.impl.AbstractCommand;
import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.cmd.impl.GenericKeyCommand;
import com.moilioncircle.redis.replicator.cmd.impl.GenericKeyValueCommand;
import com.moilioncircle.redis.replicator.rdb.datatype.AuxField;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.parser.DefaultDumpValueParser;
import com.moilioncircle.redis.replicator.util.ByteArrayList;
import com.moilioncircle.redis.replicator.util.ByteArrayMap;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.redis.constant.ValueTypeEnum;
import io.tapdata.connector.redis.exception.RedisExceptionCollector;
import io.tapdata.connector.redis.util.RedisUtil;
import io.tapdata.connector.redis.writer.*;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_redis.json")
public class RedisConnector extends ConnectorBase {

    private final static String INIT_TABLE_NAME = "tapdata";
    private final static String INIT_FIELD_KEY_NAME = "redis_key";
    private final static String INIT_FIELD_VALUE_NAME = "redis_value";
    private RedisConfig redisConfig;
    private RedisContext redisContext;
    private RedisExceptionCollector redisExceptionCollector;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        EmptyKit.closeQuietly(redisContext);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        TapTable dbTable = table(INIT_TABLE_NAME);
        dbTable.add(field(INIT_FIELD_KEY_NAME, "string"));
        dbTable.add(field(INIT_FIELD_VALUE_NAME, "string"));
        consumer.accept(Collections.singletonList(dbTable));
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return 1;
    }

    public void initConnection(TapConnectionContext connectionContext) throws Throwable {
        this.redisConfig = new RedisConfig();
        redisConfig.load(connectionContext.getConnectionConfig());
        redisConfig.load(connectionContext.getNodeConfig());
        this.redisContext = new RedisContext(redisConfig);
        this.redisExceptionCollector = new RedisExceptionCollector();
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a source, please implement at least one of batchReadFunction or streamReadFunction.
     * To be as a target, please implement WriteRecordFunction.
     * To be as a source and target, please implement the functions that source and target required.
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportWriteRecord(this::writeRecord);
//        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);

        // TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        redisConfig = new RedisConfig().load(connectionContext.getConnectionConfig());
        RedisTest redisTest = new RedisTest(redisConfig);
        TestItem testHostPort = redisTest.testHostPort();
        consumer.accept(testHostPort);
        if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
            redisTest.close();
            return null;
        }
        TestItem testConnect = redisTest.testConnect();
        consumer.accept(testConnect);
        redisTest.close();
        return null;
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        AbstractRedisRecordWriter recordWriter;
        switch (ValueTypeEnum.fromString(redisConfig.getValueType())) {
            case LIST:
                recordWriter = new ListRedisRecordWriter(redisContext, tapTable);
                break;
            case HASH:
                recordWriter = new HashRedisRecordWriter(redisContext, tapTable);
                break;
            case REDIS:
                recordWriter = new RedisCopyRecordWriter(redisContext, tapTable);
                break;
            default:
                recordWriter = new StringRedisRecordWriter(redisContext, tapTable);
        }
        try {
            recordWriter.write(tapRecordEvents, writeListResultConsumer);
        } catch (Throwable e) {
            redisExceptionCollector.collectRedisServerUnavailable(e);
            throw e;
        }
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        if (redisConfig.getOneKey()) {
            cleanOneKey(tapClearTableEvent.getTableId());
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        if (redisConfig.getOneKey()) {
            cleanOneKey(tapDropTableEvent.getTableId());
        }
    }

    private void cleanOneKey(String keyName) {
        if (EmptyKit.isBlank(keyName)) {
            return;
        }
        try (CommonJedis jedis = redisContext.getJedis()) {
            jedis.del(keyName);
            if (ValueTypeEnum.fromString(redisConfig.getValueType()) == ValueTypeEnum.HASH) {
                jedis.hdel(redisConfig.getSchemaKey(), keyName);
            }
        }
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent createTableEvent) {
        switch (ValueTypeEnum.fromString(redisConfig.getValueType())) {
            case STRING:
            case SET:
            case ZSET:
                return;
        }
        if (!redisConfig.getListHead()) {
            return;
        }
        try (CommonJedis jedis = redisContext.getJedis()) {
            List<String> fieldList = createTableEvent.getTable().getNameFieldMap().entrySet().stream().sorted(Comparator.comparing(v ->
                    EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(Map.Entry::getKey).collect(Collectors.toList());
            if (redisConfig.getOneKey()) {
                String keyName = createTableEvent.getTableId();
                if (ValueTypeEnum.fromString(redisConfig.getValueType()) == ValueTypeEnum.LIST) {
                    jedis.del(keyName);
                    jedis.rpush(keyName, String.join(EmptyKit.isEmpty(redisConfig.getValueJoinString()) ? "," : redisConfig.getValueJoinString(), fieldList));
                } else {
                    jedis.hset(redisConfig.getSchemaKey(), keyName, fieldList.stream().map(v -> csvFormat(v, ",")).collect(Collectors.joining(",")));
                }
            }
        }
    }

    private String csvFormat(String str, String delimiter) {
        if (str.contains(delimiter)
                || str.contains("\t")
                || str.contains("\r")
                || str.contains("\n")
                || str.contains(" ")
                || str.contains("\"")) {
            return "\"" + str.replaceAll("\"", "\"\"") + "\"";
        }
        return str;
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String offsetStr;
        if (EmptyKit.isNull(offsetState)) {
            offsetStr = "";
        } else {
            RedisOffset redisOffset = (RedisOffset) offsetState;
            offsetStr = "&replOffset=" + redisOffset.getOffsetV1() + "&replId=" + redisOffset.getReplId();
        }
        try (
                Replicator redisReplicator = new RedisReplicator(redisConfig.getReplicatorUri() + offsetStr)
        ) {
            RedisUtil.dress(redisReplicator);
            AtomicReference<Throwable> throwable = new AtomicReference<>();
            AtomicReference<String> replId = new AtomicReference<>();
            AtomicLong offsetV1 = new AtomicLong();
            List<TapEvent> eventList = TapSimplify.list();
            DefaultDumpValueParser parser = new DefaultDumpValueParser(redisReplicator);
            redisReplicator.addEventListener((replicator, event) -> {
                if (!isAlive() || event instanceof AbstractCommand) {
                    ErrorKit.ignoreAnyError(redisReplicator::close);
                }
                try {
                    if (event instanceof DumpKeyValuePair) {
                        DumpKeyValuePair dumpEvent = (DumpKeyValuePair) event;
                        if (dumpEvent.getDb().getDbNumber() != redisConfig.getDatabase()) {
                            return;
                        }
                        Map<String, Object> after = TapSimplify.map(TapSimplify.entry("redis_key", new String(dumpEvent.getKey())));
                        if (dumpEvent.getValue().length > 10240) {
                            after.put("redis_value", "value too large, check info");
                        } else {
                            after.put("redis_value", dumpValueToJson(parser.parse(dumpEvent).getValue()));
                        }
                        TapRecordEvent recordEvent = new TapInsertRecordEvent().init().table(tapTable.getId()).after(after);
                        recordEvent.addInfo("redis_event", dumpEvent);
                        offsetV1.set(dumpEvent.getContext().getOffsets().getV1());
                        eventList.add(recordEvent);
                        if (eventList.size() >= eventBatchSize) {
                            eventsOffsetConsumer.accept(eventList, new RedisOffset(replId.get(), offsetV1.get()));
                            eventList.clear();
                        }
                    } else if (event instanceof AuxField) {
                        AuxField auxField = (AuxField) event;
                        if ("repl-id".equals(auxField.getAuxKey())) {
                            replId.set(auxField.getAuxValue());
                        }
                    }
                } catch (Exception e) {
                    throwable.set(e);
                    ErrorKit.ignoreAnyError(redisReplicator::close);
                }
            });
            redisReplicator.open();
            if (EmptyKit.isNotEmpty(eventList)) {
                eventsOffsetConsumer.accept(eventList, new RedisOffset(replId.get(), offsetV1.get()));
            }
            if (EmptyKit.isNotNull(throwable.get())) {
                throw throwable.get();
            }
            tapConnectorContext.getStateMap().put("redisOffset", toJson(new RedisOffset(replId.get(), offsetV1.get())));
        }
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        RedisOffset redisOffset;
        if (EmptyKit.isNull(((RedisOffset) offsetState).getReplId())) {
            String stateMapOffset = (String) nodeContext.getStateMap().get("redisOffset");
            redisOffset = EmptyKit.isNull(stateMapOffset) ? null : fromJson(stateMapOffset, RedisOffset.class);
        } else {
            redisOffset = (RedisOffset) offsetState;
        }
        String offsetStr;
        if (EmptyKit.isNull(redisOffset)) {
            offsetStr = "";
        } else {
            offsetStr = "&replOffset=" + redisOffset.getOffsetV1() + "&replId=" + redisOffset.getReplId();
        }
        try (
                Replicator redisReplicator = new RedisReplicator(redisConfig.getReplicatorUri() + offsetStr)
        ) {
            RedisUtil.dress(redisReplicator);
            AtomicReference<Throwable> throwable = new AtomicReference<>();
            AtomicReference<String> replId = new AtomicReference<>();
            AtomicLong offsetV1 = new AtomicLong();
            List<TapEvent> eventList = TapSimplify.list();
            AtomicInteger dbNumber = new AtomicInteger();
            redisReplicator.addEventListener((replicator, event) -> {
                if (!isAlive()) {
                    ErrorKit.ignoreAnyError(redisReplicator::close);
                }
                try {
                    if (event instanceof DefaultCommand) {
                        DefaultCommand commandEvent = (DefaultCommand) event;
                        String command = new String(commandEvent.getCommand());
                        if ("SELECT".equals(command)) {
                            dbNumber.set(Integer.parseInt(new String(commandEvent.getArgs()[0])));
                            return;
                        }
                        if (dbNumber.get() != redisConfig.getDatabase()) {
                            return;
                        }
                        Command redisCommand = replicator.getCommandParser(CommandName.name(command)).parse(commandEvent.getArgs());
                        Map<String, Object> after;
                        if (redisCommand instanceof GenericKeyValueCommand) {
                            after = TapSimplify.map(TapSimplify.entry("redis_key", new String(((GenericKeyValueCommand) redisCommand).getKey())));
                            if (((GenericKeyValueCommand) redisCommand).getValue().length > 10240) {
                                after.put("redis_value", "value too large, check info");
                            } else {
                                after.put("redis_value", new String(((GenericKeyValueCommand) redisCommand).getValue()));
                            }
                        } else if (redisCommand instanceof GenericKeyCommand) {
                            after = TapSimplify.map(TapSimplify.entry("redis_key", new String(((GenericKeyCommand) redisCommand).getKey())));
                            after.put("redis_value", command + "(" + Arrays.stream(commandEvent.getArgs()).map(String::new).collect(Collectors.joining(", ")) + ")");
                        } else {
                            after = TapSimplify.map(TapSimplify.entry("redis_key", command));
                            after.put("redis_value", command + "(" + Arrays.stream(commandEvent.getArgs()).map(String::new).collect(Collectors.joining(", ")) + ")");
                        }
                        TapRecordEvent recordEvent = new TapInsertRecordEvent().init().table(INIT_TABLE_NAME).after(after).referenceTime(System.currentTimeMillis());
                        recordEvent.addInfo("redis_event", commandEvent);
                        offsetV1.set(commandEvent.getContext().getOffsets().getV1());
                        eventList.add(recordEvent);
                        if (eventList.size() >= recordSize) {
                            consumer.accept(eventList, new RedisOffset(replId.get(), offsetV1.get()));
                            eventList.clear();
                        }
                    } else if (event instanceof AuxField) {
                        AuxField auxField = (AuxField) event;
                        if ("repl-id".equals(auxField.getAuxKey())) {
                            replId.set(auxField.getAuxValue());
                        }
                    }
                } catch (Exception e) {
                    throwable.set(e);
                    ErrorKit.ignoreAnyError(redisReplicator::close);
                }
            });
            redisReplicator.open();
            if (EmptyKit.isNotEmpty(eventList)) {
                consumer.accept(eventList, new RedisOffset(replId.get(), offsetV1.get()));
            }
            if (EmptyKit.isNotNull(throwable.get())) {
                throw throwable.get();
            }
        }
    }

    private String dumpValueToJson(Object obj) {
        if (obj instanceof ByteArrayMap) {
            Map<String, String> map = new HashMap<>();
            ((ByteArrayMap) obj).forEach((k, v) -> map.put(new String(k), new String(v)));
            return toJson(map);
        } else if (obj instanceof ByteArrayList) {
            List<String> list = new ArrayList<>();
            ((ByteArrayList) obj).forEach(v -> list.add(new String(v)));
            return toJson(list);
        } else if (obj instanceof byte[]) {
            return new String((byte[]) obj);
        } else {
            return toJson(obj);
        }
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) {
        try (CommonJedis jedis = redisContext.getJedis()) {
            return jedis.countKeys();
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        return new RedisOffset();
    }
}
