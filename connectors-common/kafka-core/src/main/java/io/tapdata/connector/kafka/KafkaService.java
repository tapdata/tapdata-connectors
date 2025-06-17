package io.tapdata.connector.kafka;

import com.google.common.collect.Lists;
import io.tapdata.common.AbstractMqService;
import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.kafka.admin.Admin;
import io.tapdata.connector.kafka.admin.DefaultAdmin;
import io.tapdata.connector.kafka.config.*;
import io.tapdata.connector.kafka.data.KafkaOffset;
import io.tapdata.connector.kafka.util.*;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.exception.StopException;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.header.internals.RecordHeaders;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KafkaService extends AbstractMqService {

    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private String connectorId;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata"); //script factory

    public KafkaService() {

    }

    public KafkaService(KafkaConfig mqConfig, Log tapLogger) {
        this.mqConfig = mqConfig;
        this.tapLogger = tapLogger;
        ProducerConfiguration producerConfiguration = new ProducerConfiguration(mqConfig, connectorId);
        try {
            kafkaProducer = new KafkaProducer<>(producerConfiguration.build());
        } catch (Exception e) {
            e.printStackTrace();
            tapLogger.error("Kafka producer error: " + ErrorKit.getLastCause(e).getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void setConnectorId(String connectorId) {
        this.connectorId = connectorId;
    }

    public String getConnectorId() {
        return connectorId;
    }

    @Override
    protected <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) {
        return null;
    }

    @Override
    public TestItem testConnect() {
        if (((KafkaConfig) mqConfig).getKrb5()) {
            try {
                Krb5Util.checkKDCDomainsBase64(((KafkaConfig) mqConfig).getKrb5Conf());
                return new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
            } catch (Exception e) {
                return new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_FAILED, e.getMessage());
            }
        }
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        try (Admin admin = new DefaultAdmin(configuration)) {
            if (admin.isClusterConnectable()) {
                return new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
            } else {
                return new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, "cluster is not connectable");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, "when connect to cluster, error occurred " + e.getMessage());
        }
    }

    @Override
    public ConnectionCheckItem testConnection() {
        long start = System.currentTimeMillis();
        ConnectionCheckItem connectionCheckItem = ConnectionCheckItem.create();
        connectionCheckItem.item(ConnectionCheckItem.ITEM_CONNECTION);
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        try (
                Admin admin = new DefaultAdmin(configuration)
        ) {
            if (admin.isClusterConnectable()) {
                connectionCheckItem.result(ConnectionCheckItem.RESULT_SUCCESSFULLY);
            } else {
                connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information("cluster is not connectable");
            }
        } catch (Exception e) {
            connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information(e.getMessage());
        }
        connectionCheckItem.takes(System.currentTimeMillis() - start);
        return connectionCheckItem;
    }

    @Override
    public void init() {

    }

    @Override
    public int countTables() throws Throwable {
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        try (
                Admin admin = new DefaultAdmin(configuration)
        ) {
            Set<String> topicSet = admin.listTopics();
            if (EmptyKit.isEmpty(mqConfig.getMqTopicSet())) {
                return topicSet.size();
            } else {
                return (int) topicSet.stream().filter(topic -> mqConfig.getMqTopicSet().stream().anyMatch(reg -> StringKit.matchReg(topic, reg))).count();
            }
        }
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        Set<String> destinationSet = new HashSet<>();
        try (
                Admin admin = new DefaultAdmin(configuration)
        ) {
            Set<String> existTopicSet = admin.listTopics();
            if (EmptyKit.isEmpty(mqConfig.getMqTopicSet())) {
                destinationSet.addAll(existTopicSet);
            } else {
                //query queue which exists
                for (String topic : existTopicSet) {
                    if (mqConfig.getMqTopicSet().stream().anyMatch(reg -> StringKit.matchReg(topic, reg))) {
                        destinationSet.add(topic);
                    }
                }
            }
        }
        SchemaConfiguration schemaConfiguration = new SchemaConfiguration(((KafkaConfig) mqConfig), connectorId);
        multiThreadDiscoverSchema(new ArrayList<>(destinationSet), tableSize, consumer, schemaConfiguration);
    }

    protected void multiThreadDiscoverSchema(List<String> tables, int tableSize, Consumer<List<TapTable>> consumer, SchemaConfiguration schemaConfiguration) {
        CopyOnWriteArraySet<List<String>> tableLists = new CopyOnWriteArraySet<>(DbKit.splitToPieces(tables, tableSize));
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(20);
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        try {
            for (int i = 0; i < 20; i++) {
                executorService.submit(() -> {
                    try {
                        List<String> subList;
                        while ((subList = getOutTableList(tableLists)) != null) {
                            submitPageTables(tableSize, consumer, schemaConfiguration, subList);
                        }
                    } catch (Exception e) {
                        throwable.set(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (EmptyKit.isNotNull(throwable.get())) {
                throw new RuntimeException(throwable.get());
            }
        } finally {
            executorService.shutdown();
        }
    }

    private synchronized List<String> getOutTableList(CopyOnWriteArraySet<List<String>> tableLists) {
        if (EmptyKit.isNotEmpty(tableLists)) {
            List<String> list = tableLists.stream().findFirst().orElseGet(ArrayList::new);
            tableLists.remove(list);
            return list;
        }
        return null;
    }

    protected synchronized void syncSchemaSubmit(List<TapTable> tapTables, Consumer<List<TapTable>> consumer) {
        consumer.accept(tapTables);
    }

    protected void submitPageTables(int tableSize, Consumer<List<TapTable>> consumer, SchemaConfiguration schemaConfiguration, List<String> destinationSet) {
        List<List<String>> tablesList = Lists.partition(destinationSet, tableSize);
        Map<String, Object> config = schemaConfiguration.build();
        try (
                KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(config)
        ) {
            tablesList.forEach(tables -> {
                List<TapTable> tableList = new ArrayList<>();
                List<String> topics = new ArrayList<>(tables);
                kafkaConsumer.subscribe(topics);
                ConsumerRecords<byte[], byte[]> consumerRecords;
                while (!(consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2L))).isEmpty()) {
                    for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                        if (!topics.contains(record.topic())) {
                            continue;
                        }
                        Map<String, Object> messageBody;
                        try {
                            messageBody = jsonParser.fromJsonBytes(record.value(), Map.class);
                            if (messageBody == null) {
                                tapLogger.warn("messageBody not allow null...");
                                continue;
                            }
                            if (messageBody.containsKey("mqOp")) {
                                messageBody = (Map<String, Object>) messageBody.get("data");
                            }
                        } catch (Exception e) {
                            tapLogger.warn("topic[{}] value [{}] can not parse to json, ignore...", record.topic(), new String(record.value()));
                            TapTable tapTable = new TapTable(record.topic());
                            tableList.add(tapTable);
                            topics.remove(record.topic());
                            continue;
                        }
                        try {
                            TapTable tapTable = new TapTable(record.topic());
                            SCHEMA_PARSER.parse(tapTable, messageBody);
                            tableList.add(tapTable);
                        } catch (Throwable t) {
                            tapLogger.warn(String.format("%s parse topic invalid json object: %s", record.topic(), t.getMessage()), t);
                        }
                        topics.remove(record.topic());
                    }
                    if (EmptyKit.isEmpty(topics)) {
                        break;
                    }
                    kafkaConsumer.subscribe(topics);
                }
                topics.stream().map(TapTable::new).forEach(tableList::add);
                syncSchemaSubmit(tableList, consumer);
            });
        }
    }

    public static Object executeScript(ScriptEngine scriptEngine, String function, Object... params) {
        if (scriptEngine != null) {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                return invocable.invokeFunction(function, params);
            } catch (StopException e) {
//                TapLogger.info(TAG, "Get data and stop script.");
                throw new RuntimeException(e);
            } catch (ScriptException | NoSuchMethodException | RuntimeException e) {
//                TapLogger.error(TAG, "Run script error, message: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) {
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        CountDownLatch countDownLatch = new CountDownLatch(tapRecordEvents.size());
        try {
            for (TapRecordEvent event : tapRecordEvents) {
                if (null != isAlive && !isAlive.get()) {
                    break;
                }
                Map<String, Object> data;
                MqOp mqOp = MqOp.INSERT;
                if (event instanceof TapInsertRecordEvent) {
                    data = ((TapInsertRecordEvent) event).getAfter();
                } else if (event instanceof TapUpdateRecordEvent) {
                    data = ((TapUpdateRecordEvent) event).getAfter();
                    mqOp = MqOp.UPDATE;
                } else if (event instanceof TapDeleteRecordEvent) {
                    data = ((TapDeleteRecordEvent) event).getBefore();
                    mqOp = MqOp.DELETE;
                } else {
                    data = new HashMap<>();
                }
                byte[] body = jsonParser.toJsonBytes(data, JsonParser.ToJsonFeature.WriteMapNullValue);
                MqOp finalMqOp = mqOp;
                Callback callback = (metadata, exception) -> {
                    try {
                        if (EmptyKit.isNotNull(exception)) {
                            listResult.addError(event, exception);
                        }
                        switch (finalMqOp) {
                            case INSERT:
                                insert.incrementAndGet();
                                break;
                            case UPDATE:
                                update.incrementAndGet();
                                break;
                            case DELETE:
                                delete.incrementAndGet();
                                break;
                        }
                    } finally {
                        countDownLatch.countDown();
                    }
                };
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapTable.getId(),
                        null, event.getTime(), getKafkaMessageKey(data, tapTable), body,
                        new RecordHeaders().add("mqOp", mqOp.getOp().getBytes()));
                kafkaProducer.send(producerRecord, callback);
            }
        } catch (RejectedExecutionException e) {
            tapLogger.warn("task stopped, some data produce failed!", e);
        } catch (Exception e) {
            tapLogger.error("produce error, or task interrupted!", e);
        }
        try {
            while (null != isAlive && isAlive.get()) {
                if (countDownLatch.await(500L, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            tapLogger.error("error occur when await", e);
        } finally {
            writeListResultConsumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
        }
//            this.produce(null,tapRecordEvents,tapTable,writeListResultConsumer,isAlive);
    }

    @Override
    public void produce(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) {
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        CountDownLatch countDownLatch = new CountDownLatch(tapRecordEvents.size());
        ScriptEngine scriptEngine;
        String script = ((KafkaConfig) mqConfig).getScript();
        Map<String, Object> record = new HashMap<>();
        try {
            scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT,
                    new ScriptOptions().engineName("graal.js"));
            String buildInMethod = initBuildInMethod();
            String scripts = script + System.lineSeparator() + buildInMethod;
            scriptEngine.eval(scripts);
        } catch (Exception e) {
            throw new CoreException("Engine initialization failed!");
        }
        try {
            for (TapRecordEvent event : tapRecordEvents) {
                if (null != isAlive && !isAlive.get()) {
                    break;
                }
                Map<String, Object> data;
                Map<String, Map<String, Object>> allData = new HashMap();
                MqOp mqOp = MqOp.INSERT;
                if (event instanceof TapInsertRecordEvent) {
                    data = ((TapInsertRecordEvent) event).getAfter();
                    allData.put("before", new HashMap<String, Object>());
                    allData.put("after", data);
                } else if (event instanceof TapUpdateRecordEvent) {
                    data = ((TapUpdateRecordEvent) event).getAfter();
                    Map<String, Object> before = ((TapUpdateRecordEvent) event).getBefore();
                    allData.put("before", null == before ? new HashMap<>() : before);
                    allData.put("after", data);
                    mqOp = MqOp.UPDATE;
                } else if (event instanceof TapDeleteRecordEvent) {
                    data = ((TapDeleteRecordEvent) event).getBefore();
                    allData.put("before", data);
                    allData.put("after", new HashMap<String, Object>());
                    mqOp = MqOp.DELETE;
                } else {
                    data = new HashMap<>();
                }
                byte[] kafkaMessageKey = getKafkaMessageKey(data, tapTable);
                record.put("data", allData);
                Map<String, Object> header = new HashMap<>();
                header.put("mqOp", mqOp.getOp());
                record.put("header", header);
                String op = mqOp.getOp();
                Collection<String> conditionKeys = tapTable.primaryKeys(true);
                RecordHeaders recordHeaders = new RecordHeaders();
                byte[] body = {};
                Object eventObj = ObjectUtils.covertData(executeScript(scriptEngine, "process", record, op, conditionKeys));
                if (null == eventObj) {
                    continue;
                } else {
                    Map<String, Object> res = (Map<String, Object>) eventObj;
                    if (null == res.get("data")) {
                        throw new RuntimeException("data cannot be null");
                    } else {
                        Object obj = res.get("data");
                        if (obj instanceof Map) {
                            Map<String, Map<String, Object>> map = (Map<String, Map<String, Object>>) res.get("data");
                            if (map.containsKey("before") && map.get("before").isEmpty()) {
                                map.remove("before");
                            }
                            if (map.containsKey("after") && map.get("after").isEmpty()) {
                                map.remove("after");
                            }
                            res.put("data", map);
                            body = jsonParser.toJsonBytes(res.get("data"), JsonParser.ToJsonFeature.WriteMapNullValue);
                        } else {
                            body = obj.toString().getBytes();
                        }
                    }
                    if (res.containsKey("header")) {
                        Object obj = res.get("header");
                        if (obj instanceof Map) {
                            Map<String, Object> head = (Map<String, Object>) res.get("header");
                            for (String s : head.keySet()) {
                                recordHeaders.add(s, head.get(s).toString().getBytes());
                            }
                        } else {
                            throw new RuntimeException("header must be a collection type");
                        }
                    } else {
                        recordHeaders.add("mqOp", mqOp.toString().getBytes());
                    }
                }
                MqOp finalMqOp = mqOp;
                Callback callback = (metadata, exception) -> {
                    try {
                        if (EmptyKit.isNotNull(exception)) {
                            listResult.addError(event, exception);
                        }
                        switch (finalMqOp) {
                            case INSERT:
                                insert.incrementAndGet();
                                break;
                            case UPDATE:
                                update.incrementAndGet();
                                break;
                            case DELETE:
                                delete.incrementAndGet();
                                break;
                        }
                    } finally {
                        countDownLatch.countDown();
                    }
                };
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapTable.getId(),
                        null, event.getTime(), kafkaMessageKey, body,
                        recordHeaders);
                kafkaProducer.send(producerRecord, callback);
            }
        } catch (RejectedExecutionException e) {
            tapLogger.warn("task stopped, some data produce failed!", e);
        } catch (Exception e) {
            tapLogger.error("produce error, or task interrupted!", e);
        }
        try {
            while (null != isAlive && isAlive.get()) {
                if (countDownLatch.await(500L, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            tapLogger.error("error occur when await", e);
        } finally {
            writeListResultConsumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
        }
    }

    protected String initBuildInMethod() {
        StringBuilder buildInMethod = new StringBuilder();
        buildInMethod.append("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n");
        buildInMethod.append("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
        buildInMethod.append("var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
        buildInMethod.append("var HashMap = Java.type(\"java.util.HashMap\");\n");
        buildInMethod.append("var LinkedHashMap = Java.type(\"java.util.LinkedHashMap\");\n");
        buildInMethod.append("var ArrayList = Java.type(\"java.util.ArrayList\");\n");
        buildInMethod.append("var Date = Java.type(\"java.util.Date\");\n");
        buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
        buildInMethod.append("var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n");
        buildInMethod.append("var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n");
        buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
        buildInMethod.append("var util = Java.type(\"com.tapdata.processor.util.Util\");\n");
        buildInMethod.append("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n");
        buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
        buildInMethod.append("var Collections = Java.type(\"java.util.Collections\");\n");
        buildInMethod.append("var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n");
        buildInMethod.append("var sleep = function(ms){\n" +
                "var Thread = Java.type(\"java.lang.Thread\");\n" +
                "Thread.sleep(ms);\n" +
                "}\n");
        return buildInMethod.toString();
    }

    @Override
    public void produce(TapFieldBaseEvent tapFieldBaseEvent) {
        AtomicReference<Throwable> reference = new AtomicReference<>();
        String tableId = tapFieldBaseEvent.getTableId();
        ScriptEngine scriptEngine;
        String script = ((KafkaConfig) mqConfig).getScript();
        Map<String, Object> record = new HashMap<>();
        record.put("data", tapFieldBaseEvent.getOriginDDL());
        record.put("tableName", tableId);
        byte[] body;
        if (((KafkaConfig) mqConfig).getEnableScript()) {
            try {
                scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT,
                        new ScriptOptions().engineName("graal.js"));
                String buildInMethod = initBuildInMethod();
                String scripts = script + System.lineSeparator() + buildInMethod;
                scriptEngine.eval(scripts);
            } catch (Exception e) {
                throw new CoreException("Engine initialization failed!");
            }
            Object eventObj = ObjectUtils.covertData(executeScript(scriptEngine, "process", record, MqOp.DDL.getOp(), Collections.emptyList()));
            if (eventObj instanceof Map) {
                body = jsonParser.toJsonBytes(eventObj);
            } else if (eventObj == null) {
                return;
            } else {
                body = eventObj.toString().getBytes();
            }
        } else {
            body = tapFieldBaseEvent.getOriginDDL().toString().getBytes();
        }
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapFieldBaseEvent.getTableId(),
                null, tapFieldBaseEvent.getTime(), null, body,
                new RecordHeaders()
                        .add("mqOp", MqOp.DDL.getOp().getBytes())
                        .add("eventClass", tapFieldBaseEvent.getClass().getName().getBytes()));
        Callback callback = (metadata, exception) -> reference.set(exception);
        kafkaProducer.send(producerRecord, callback);
        if (EmptyKit.isNotNull(reference.get())) {
            throw new RuntimeException(reference.get());
        }
    }

    private byte[] getKafkaMessageKey(Map<String, Object> data, TapTable tapTable) {
        if (EmptyKit.isEmpty(tapTable.primaryKeys(true))) {
            return null;
        } else {
            return jsonParser.toJsonBytes(tapTable.primaryKeys(true).stream().map(key -> String.valueOf(data.get(key))).collect(Collectors.joining("_")));
        }
    }

    //kafka查询topic的partition数量
    private List<Integer> getPartitionList(String topic) {
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        try (
                Admin admin = new DefaultAdmin(configuration)
        ) {
            List<TopicPartitionInfo> partitionInfos = admin.getTopicPartitionInfo(topic);
            return partitionInfos.stream().map(TopicPartitionInfo::partition).collect(Collectors.toList());
        } catch (Exception e) {
            tapLogger.warn("get partition count error", e);
        }
        return Collections.emptyList();
    }

    @Override
    public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        consuming.set(true);
        String tableName = tapTable.getId();
        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration(((KafkaConfig) mqConfig), connectorId, true);
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        List<Integer> partitionInfo = getPartitionList(tableName);
        int threadSize = Math.min(Math.max(partitionInfo.size(), 1), 8);
        List<List<Integer>> partitionGroup = DbKit.splitToPieces(partitionInfo, (partitionInfo.size() - 1) / threadSize + 1);
        CountDownLatch countDownLatch = new CountDownLatch(threadSize);
        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
        try {
            for (int i = 0; i < threadSize; i++) {
                List<Integer> partitions = partitionGroup.get(i);
                executorService.submit(() -> {
                    try (KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerConfiguration.build())) {
                        List<TapEvent> list = TapSimplify.list();
                        if (threadSize > 1) {
                            kafkaConsumer.assign(partitions.stream().map(v -> new TopicPartition(tapTable.getId(), v)).collect(Collectors.toList()));
                        } else {
                            kafkaConsumer.subscribe(Collections.singleton(tapTable.getId()));
                        }
                        while (consuming.get()) {
                            ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(6L));
                            if (consumerRecords.isEmpty()) {
                                break;
                            }
                            for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                                makeMessage(consumerRecord, tableName, list::add);
                                if (list.size() >= eventBatchSize) {
                                    syncEventSubmit(list, eventsOffsetConsumer);
                                    list = TapSimplify.list();
                                }
                            }
                        }
                        if (EmptyKit.isNotEmpty(list)) {
                            syncEventSubmit(list, eventsOffsetConsumer);
                        }
                    } catch (Exception e) {
                        throwable.set(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (EmptyKit.isNotNull(throwable.get())) {
                throw new RuntimeException(throwable.get());
            }
        } finally {
            executorService.shutdown();
        }
    }

    private synchronized void syncEventSubmit(List<TapEvent> eventList, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        eventsOffsetConsumer.accept(eventList, TapSimplify.list());
    }

    @Override
	public void streamConsume(List<String> tableList, Object offset, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        consuming.set(true);
        int maxDelay = 500;
        KafkaConfig kafkaConfig = (KafkaConfig) mqConfig;
        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration((kafkaConfig), connectorId, true);
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerConfiguration.build())) {
            KafkaOffset streamOffset = KafkaOffsetUtils.setConsumerByOffset(kafkaConsumer, tableList, offset, consuming);
            try (BatchPusher<TapEvent> batchPusher = new BatchPusher<TapEvent>(
                tapEvents -> eventsOffsetConsumer.accept(tapEvents, streamOffset.clone())
            ).batchSize(eventBatchSize).maxDelay(maxDelay)) {
                // 将初始化的 offset 推送到目标，让指定时间的增量任务下次启动时拿到 offset
                Optional.of(new HeartbeatEvent()).ifPresent(event -> {
                    event.setTime(System.currentTimeMillis());
                    batchPusher.add(event);
                });

                // 消费数据
                while (consuming.get()) {
                    ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2L));
                    if (consumerRecords.isEmpty()) {
                        batchPusher.checkAndSummit();
                    } else {
                        for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                            streamOffset.addTopicOffset(consumerRecord); // 推进 offset
                            makeMessage(consumerRecord, consumerRecord.topic(), batchPusher::add);
                        }
                    }
                }
            }
        } catch (InterruptedException | InterruptException ex) {
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            tapLogger.error("Stream consume occur: {}", ex.getMessage(), ex);
        }
    }

    private void makeMessage(ConsumerRecord<byte[], byte[]> consumerRecord, String tableName, Consumer<TapEvent> consumer) {
        AtomicReference<String> mqOpReference = new AtomicReference<>();
        mqOpReference.set(MqOp.INSERT.getOp());
        consumerRecord.headers().headers("mqOp").forEach(header -> mqOpReference.set(new String(header.value())));
        if (MqOp.fromValue(mqOpReference.get()) == MqOp.DDL) {
            consumerRecord.headers().headers("eventClass").forEach(eventClass -> {
                TapFieldBaseEvent tapFieldBaseEvent;
                try {
                    tapFieldBaseEvent = (TapFieldBaseEvent) jsonParser.fromJsonBytes(consumerRecord.value(), Class.forName(new String(eventClass.value())));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                consumer.accept(tapFieldBaseEvent);
            });
        } else {
            Map<String, Object> data = jsonParser.fromJsonBytes(consumerRecord.value(), Map.class);
            long referenceTime = consumerRecord.timestamp();
            switch (MqOp.fromValue(mqOpReference.get())) {
                case INSERT:
                    consumer.accept(new TapInsertRecordEvent().init().table(tableName).after(data).referenceTime(referenceTime));
                    break;
                case UPDATE:
                    consumer.accept(new TapUpdateRecordEvent().init().table(tableName).after(data).referenceTime(referenceTime));
                    break;
                case DELETE:
                    consumer.accept(new TapDeleteRecordEvent().init().table(tableName).before(data).referenceTime(referenceTime));
                    break;
            }
        }
    }

    @Override
    public void close() {
        super.close();
        if (EmptyKit.isNotNull(kafkaProducer)) {
            kafkaProducer.close();
        }
    }
}
