package io.tapdata.connector.redis;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.impl.AbstractCommand;
import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.cmd.impl.PingCommand;
import com.moilioncircle.redis.replicator.rdb.datatype.AuxField;
import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.parser.DefaultDumpValueParser;
import com.moilioncircle.redis.replicator.util.ByteArrayList;
import com.moilioncircle.redis.replicator.util.ByteArrayMap;
import com.moilioncircle.redis.replicator.util.ByteArraySet;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.redis.bean.ZSetElement;
import io.tapdata.connector.redis.constant.DeployModeEnum;
import io.tapdata.connector.redis.constant.ValueTypeEnum;
import io.tapdata.connector.redis.exception.RedisExceptionCollector;
import io.tapdata.connector.redis.sentinel.RedisSentinelReplicator;
import io.tapdata.connector.redis.sentinel.RedisSentinelURI;
import io.tapdata.connector.redis.util.RedisUtil;
import io.tapdata.connector.redis.writer.*;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
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
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
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
    private final byte[] publishBytes = "PUBLISH".getBytes(StandardCharsets.UTF_8);
    private final byte[] helloBytes = "__sentinel__:hello".getBytes(StandardCharsets.UTF_8);
    private RedisConfig redisConfig;
    private RedisContext redisContext;
    private RedisExceptionCollector redisExceptionCollector;
    private String firstConnectorId;

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
        isConnectorStarted(connectionContext, connectorContext -> {
            firstConnectorId = (String) connectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = connectionContext.getId();
                connectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
            redisConfig.load(connectionContext.getNodeConfig());
        });
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
        RedisOffset redisOffset = (RedisOffset) offsetState;
        try (
                Replicator redisReplicator = generateReplicator(redisOffset)
        ) {
            RedisUtil.dress(redisReplicator);
            AtomicReference<Throwable> throwable = new AtomicReference<>();
            AtomicReference<String> replId = new AtomicReference<>();
            AtomicLong offsetV1 = new AtomicLong();
            List<TapEvent> eventList = TapSimplify.list();
            DefaultDumpValueParser parser = new DefaultDumpValueParser(redisReplicator);
            redisReplicator.addEventListener((replicator, event) -> {
                if (!isAlive()) {
                    EmptyKit.closeQuietly(redisReplicator);
                    return;
                }
                if (event instanceof AbstractCommand) {
                    offsetV1.set(((AbstractCommand) event).getContext().getOffsets().getV2());
                    EmptyKit.closeQuietly(redisReplicator);
                    return;
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
                    EmptyKit.closeQuietly(redisReplicator);
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
        try (
                Replicator redisReplicator = generateReplicator(redisOffset)
        ) {
            RedisUtil.dress(redisReplicator);
            AtomicReference<Throwable> throwable = new AtomicReference<>();
            AtomicReference<String> replId = new AtomicReference<>();
            AtomicLong offsetV1 = new AtomicLong();
            List<TapEvent> eventList = TapSimplify.list();
            AtomicInteger dbNumber = new AtomicInteger();
            AtomicInteger helloCount = new AtomicInteger(0);
            redisReplicator.addEventListener((replicator, event) -> {
                if (!isAlive()) {
                    EmptyKit.closeQuietly(redisReplicator);
                    return;
                }
                try {
                    if (event instanceof DefaultCommand) {
                        DefaultCommand commandEvent = (DefaultCommand) event;
                        String command = new String(commandEvent.getCommand());
                        if ("SELECT".equals(command)) {
                            dbNumber.set(Integer.parseInt(new String(commandEvent.getArgs()[0])));
                            return;
                        }
                        if (((DefaultCommand) event).getArgs().length > 0) {
                            if (Arrays.equals(publishBytes, commandEvent.getCommand()) && Arrays.equals(helloBytes, ((DefaultCommand) event).getArgs()[0])) {
                                helloCount.incrementAndGet();
                                if (helloCount.get() >= 1000) {
                                    helloCount.set(0);
                                    consumer.accept(Collections.singletonList(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis())), new RedisOffset(replId.get(), ((DefaultCommand) event).getContext().getOffsets().getV1()));
                                }
                                return;
                            }
                        }
                        if (dbNumber.get() != redisConfig.getDatabase()) {
                            return;
                        }
                        Map<String, Object> after = TapSimplify.map(TapSimplify.entry("redis_key", commandEvent.getArgs().length > 0 ? new String(commandEvent.getArgs()[0]) : command));
                        if (Arrays.stream(commandEvent.getArgs()).map(v -> v.length).reduce(Integer::sum).orElse(0) > 10240) {
                            after.put("redis_value", "value too large, check info");
                        } else {
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
                    } else if (event instanceof PingCommand) {
                        consumer.accept(Collections.singletonList(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis())), new RedisOffset(replId.get(), ((PingCommand) event).getContext().getOffsets().getV1()));
                    }
                } catch (Exception e) {
                    throwable.set(e);
                    EmptyKit.closeQuietly(redisReplicator);
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

    private Replicator generateReplicator(RedisOffset redisOffset) throws Exception {
        String offsetStr;
        if (EmptyKit.isNull(redisOffset)) {
            offsetStr = "";
        } else {
            offsetStr = "&replOffset=" + redisOffset.getOffsetV1() + "&replId=" + redisOffset.getReplId();
        }
        if (DeployModeEnum.fromString(redisConfig.getDeploymentMode()) == DeployModeEnum.STANDALONE) {
            return new RedisReplicator(redisConfig.getReplicatorUri() + offsetStr);
        } else if (DeployModeEnum.fromString(redisConfig.getDeploymentMode()) == DeployModeEnum.SENTINEL) {
            int slavePort = 40000 + firstConnectorId.hashCode() % 10000;
            String sentinelUrl = redisConfig.getReplicatorUri() + "&slavePort=" + slavePort;
            RedisSentinelURI redisSentinelURI = new RedisSentinelURI(sentinelUrl);
            final Configuration configuration = Configuration.defaultSetting();
            if (EmptyKit.isNotNull(redisOffset)) {
                configuration.setReplId(redisOffset.getReplId());
                configuration.setReplOffset(redisOffset.getOffsetV1());
            }
            configuration.setSlavePort(slavePort);
            if (StringUtils.isNotBlank(redisConfig.getPassword())) {
                configuration.setAuthPassword(redisConfig.getPassword());
            }
            configuration.setRetries(36000);
            configuration.setRetryTimeInterval(10);
            return new RedisSentinelReplicator(redisSentinelURI, configuration);
        } else {
            return new RedisReplicator(redisConfig.getReplicatorUri() + offsetStr);
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
        } else if (obj instanceof ByteArraySet) {
            Set<String> set = new HashSet<>();
            ((ByteArraySet) obj).forEach(v -> set.add(new String(v)));
            return toJson(set);
        } else if (obj instanceof LinkedHashSet) {
            Set<ZSetElement> zSet = new LinkedHashSet<>();
            ((LinkedHashSet) obj).forEach(v -> {
                ZSetEntry entry = (ZSetEntry) v;
                ZSetElement zSetElement = new ZSetElement();
                zSetElement.setElement(new String(entry.getElement()));
                zSetElement.setScore(entry.getScore());
                zSet.add(zSetElement);
            });
            return toJson(zSet);
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
