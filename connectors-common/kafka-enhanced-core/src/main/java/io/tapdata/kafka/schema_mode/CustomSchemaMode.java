package io.tapdata.kafka.schema_mode;

import io.tapdata.base.ConnectorBase;
import io.tapdata.constant.DMLType;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.exception.StopException;
import io.tapdata.kafka.AbsSchemaMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.KafkaConfig;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.tapdata.constant.DMLType.*;

public class CustomSchemaMode extends AbsSchemaMode {

    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
    // GraalVM JS Context 不允许多线程共享，使用 ThreadLocal 让 discoverSchema 等并发场景每个线程独享一个 Engine
    private final ThreadLocal<ScriptEngine> scriptEngine;
    private final String composedScript;
    // 每个 topic 已观察到的字段集合（fieldName -> tapType），用于检测新增字段；只增不减
    private final Map<String, Map<String, String>> lastSchemaPerTopic = new ConcurrentHashMap<>();

    public CustomSchemaMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.CUSTOM, kafkaService);
        KafkaConfig kafkaConfig = kafkaService.getConfig();
        String script = EmptyKit.isNotBlank(kafkaConfig.getNodeScript()) ? kafkaConfig.getNodeScript() : kafkaConfig.getConnectionScript();
        this.composedScript = script + System.lineSeparator() + initBuildInMethod();
        this.scriptEngine = ThreadLocal.withInitial(this::buildScriptEngine);
        // 在构造线程上提前 eval 一次，确保脚本语法错误能在初始化阶段暴露
        this.scriptEngine.get();
    }

    private ScriptEngine buildScriptEngine() {
        try {
            ClassLoader appClassLoader = ScriptOptions.class.getClassLoader();
            ScriptEngine engine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT,
                    new ScriptOptions().engineName("graal.js").classLoader(appClassLoader));
            engine.eval(composedScript);
            return engine;
        } catch (Exception e) {
            throw new CoreException("Engine initialization failed!");
        }
    }

    @Override
    public void sampleOneSchema(String table, TapTable sampleTable) {
        kafkaService.<String, String>sampleValue(Collections.singletonList(table), null, record -> {
            if (null != record) {
                Object eventObj = executeScript(scriptEngine.get(), "analyze", record.headers(), record.key(), record.value(), record.partition());
                if (eventObj instanceof Map) {
                    Map<String, Object> after = (Map<String, Object>) ((Map<String, Object>) eventObj).get("after");
                    if (after != null) {
                        for (Map.Entry<String, Object> entry : after.entrySet()) {
                            String fieldName = entry.getKey();
                            Object fieldValue = entry.getValue();
                            TapField tapField = new TapField(fieldName, inferTapType(fieldValue));
                            sampleTable.add(tapField);
                        }
                        return false;
                    }
                }
            }
            return true;
        });
    }

    @Override
    public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord == null || consumerRecord.value() == null) {
            return null;
        }
        try {
            Object value = executeScript(scriptEngine.get(), "analyze", consumerRecord.headers(), consumerRecord.key(), consumerRecord.value(), consumerRecord.partition());
            Map<String, Object> after = (Map<String, Object>) ((Map<String, Object>) value).get("after");
            Map<String, Object> before = (Map<String, Object>) ((Map<String, Object>) value).get("before");
            String op = String.valueOf(((Map<String, Object>) value).get("op"));
            // 根据操作类型创建对应的 TapEvent
            TapBaseEvent tapEvent;
            switch (op) {
                case "i":
                    tapEvent = TapInsertRecordEvent.create();
                    ((TapInsertRecordEvent) tapEvent).setAfter(after);
                    break;

                case "u":
                    tapEvent = TapUpdateRecordEvent.create();
                    ((TapUpdateRecordEvent) tapEvent).setAfter(after);
                    ((TapUpdateRecordEvent) tapEvent).setBefore(before);
                    break;

                case "d":
                    tapEvent = TapDeleteRecordEvent.create();
                    ((TapDeleteRecordEvent) tapEvent).setBefore(before);
                    break;

                case "ddl": {
                    if (((Map<String, Object>) value).get("event") != null) {
                        tapEvent = TapSimplify.fromJson(((Map<String, Object>) value).get("event").toString(), TapDDLEvent.class);
                    } else {
                        throw new RuntimeException("when op is ddl, event cannot be empty!");
                    }
                    break;
                }

                default:
                    tapEvent = TapInsertRecordEvent.create();
                    ((TapInsertRecordEvent) tapEvent).setAfter(after);
                    break;
            }
            // 设置事件元数据
            tapEvent.setTableId(consumerRecord.topic());
            tapEvent.setReferenceTime(consumerRecord.timestamp());
            return tapEvent;
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert JSON message to TapEvent from topic: " + consumerRecord.topic(), e);
        }
    }

    @Override
    public List<TapEvent> toTapEvents(ConsumerRecord<?, ?> consumerRecord) {
        TapEvent event = toTapEvent(consumerRecord);
        if (event == null) {
            return Collections.emptyList();
        }
        // 仅基于 INSERT / UPDATE 的 after 推断 schema 增量；DELETE 通常仅含 PK，不参与基线更新
        Map<String, Object> data = extractAfterForSchema(event);
        if (data == null || data.isEmpty()) {
            return Collections.singletonList(event);
        }
        String topic = consumerRecord.topic();
        Map<String, String> currentSchema = deriveSchemaFromMap(data);
        Map<String, String> lastSchema = lastSchemaPerTopic.get(topic);
        List<TapEvent> events = new ArrayList<>();
        if (lastSchema == null) {
            // 首次：与 tableMap 中已知的 TapTable 字段对比，避免把已有字段当作新增
            TapTable knownTable = kafkaService.getConfig().tableMapGet(topic);
            Set<String> knownFieldNames = knownTable == null || knownTable.getNameFieldMap() == null
                    ? Collections.emptySet() : knownTable.getNameFieldMap().keySet();
            appendAddedFieldEvent(events, topic, currentSchema, knownFieldNames, consumerRecord.timestamp());
            // 基线 = 已知字段名 ∪ 当前推断出的 schema；类型只对当前消息的字段记录，避免错误覆盖
            Map<String, String> baseline = new LinkedHashMap<>();
            for (String name : knownFieldNames) {
                baseline.put(name, currentSchema.getOrDefault(name, "STRING"));
            }
            baseline.putAll(currentSchema);
            lastSchemaPerTopic.put(topic, baseline);
        } else {
            appendAddedFieldEvent(events, topic, currentSchema, lastSchema.keySet(), consumerRecord.timestamp());
            // 仅追加新键，已存在的键不动；不缩减、不修改类型，避免类型抖动反复触发
            for (Map.Entry<String, String> e : currentSchema.entrySet()) {
                lastSchema.putIfAbsent(e.getKey(), e.getValue());
            }
        }
        events.add(event);
        return filterPrimaryKeyDDL(topic, events);
    }

    private Map<String, Object> extractAfterForSchema(TapEvent event) {
        if (event instanceof TapInsertRecordEvent) {
            return ((TapInsertRecordEvent) event).getAfter();
        }
        if (event instanceof TapUpdateRecordEvent) {
            return ((TapUpdateRecordEvent) event).getAfter();
        }
        return null;
    }

    private Map<String, String> deriveSchemaFromMap(Map<String, Object> data) {
        Map<String, String> schema = new LinkedHashMap<>();
        if (data == null) {
            return schema;
        }
        for (Map.Entry<String, Object> e : data.entrySet()) {
            schema.put(e.getKey(), inferTapType(e.getValue()));
        }
        return schema;
    }

    private void appendAddedFieldEvent(List<TapEvent> events, String topic, Map<String, String> currentSchema, Set<String> knownNames, long referenceTime) {
        List<TapField> addedFields = new ArrayList<>();
        for (Map.Entry<String, String> e : currentSchema.entrySet()) {
            if (knownNames.contains(e.getKey())) continue;
            TapField tf = new TapField(e.getKey(), e.getValue());
            tf.setNullable(true);
            addedFields.add(tf);
        }
        if (addedFields.isEmpty()) {
            return;
        }
        TapNewFieldEvent add = new TapNewFieldEvent();
        add.setTime(System.currentTimeMillis());
        add.setTableId(topic);
        add.setReferenceTime(referenceTime);
        add.setNewFields(addedFields);
        events.add(add);
    }



    @Override
    public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable table, TapEvent tapEvent) {
        Map<String, Object> record = new HashMap<>();
        Map<String, Object> data;
        Map<String, Object> allData = new HashMap<>();
        DMLType op = INSERT;
        if (tapEvent instanceof TapInsertRecordEvent) {
            data = ((TapInsertRecordEvent) tapEvent).getAfter();
            allData.put("before", new HashMap<String, Object>());
            allData.put("after", data);
        } else if (tapEvent instanceof TapUpdateRecordEvent) {
            data = ((TapUpdateRecordEvent) tapEvent).getAfter();
            Map<String, Object> before = ((TapUpdateRecordEvent) tapEvent).getBefore();
            allData.put("before", null == before ? new HashMap<>() : before);
            allData.put("after", data);
            op = UPDATE;
        } else if (tapEvent instanceof TapDeleteRecordEvent) {
            data = ((TapDeleteRecordEvent) tapEvent).getBefore();
            allData.put("before", data);
            allData.put("after", new HashMap<String, Object>());
            op = DELETE;
        } else {
            data = new HashMap<>();
        }
        String topic = topic(table, tapEvent);
        record.put("tableName", topic);
        String kafkaMessageKey = createKafkaKeyValueMap(data, table);
        record.put("key", kafkaMessageKey);
        record.put("data", allData);
        Map<String, Object> header = new HashMap<>();
        header.put("op", op.name());
        record.put("header", header);
        Collection<String> conditionKeys = table.primaryKeys(true);
        RecordHeaders recordHeaders = new RecordHeaders();
        String key = null;
        Object eventObj = covertData(executeScript(scriptEngine.get(), "process", record, op.name(), conditionKeys));
        String body = null;
        Integer partition = null;
        if (null == eventObj) {
        } else {
            Map<String, Object> res = (Map<String, Object>) eventObj;
            if (null == res.get("value")) {
                throw new RuntimeException("value cannot be null");
            } else {
                Object obj = res.get("value");
                if (obj instanceof Map) {
                    Map<String, Map<String, Object>> map = (Map<String, Map<String, Object>>) obj;
                    if (map.containsKey("before") && map.get("before").isEmpty()) {
                        map.remove("before");
                    }
                    if (map.containsKey("after") && map.get("after").isEmpty()) {
                        map.remove("after");
                    }
                    res.put("value", map);
                    body = TapSimplify.toJson(obj, JsonParser.ToJsonFeature.WriteMapNullValue);
                } else {
                    body = obj.toString();
                }
            }
            if (res.containsKey("header")) {
                Object obj = res.get("header");
                if (obj instanceof Map) {
                    Map<String, Object> head = (Map<String, Object>) obj;
                    for (String s : head.keySet()) {
                        recordHeaders.add(s, head.get(s).toString().getBytes());
                    }
                } else {
                    throw new RuntimeException("header must be a collection type");
                }
            } else {
                recordHeaders.add("op", op.name().getBytes());
            }
            if (res.containsKey("key")) {
                key = String.valueOf(res.get("key"));
            }
            if (res.containsKey("partition")) {
                partition = Integer.valueOf(String.valueOf(res.get("partition")));
            } else {
                partition = key == null ? null : computePartition(key.getBytes(), kafkaService.getConfig().getNodePartitionSize());
            }
        }
        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(topic, partition, tapEvent.getTime(), key, body, recordHeaders);
        return List.of(producerRecord);
    }

    public ProducerRecord<Object, Object> fromTapDDLEvent(TapDDLEvent ddlEvent) {
        Map<String, Object> record = new HashMap<>();
        Map<String, Object> allData = new HashMap<>();
        allData.put("ddl", ddlEvent.getOriginDDL());
        allData.put("event", TapSimplify.toJson(ddlEvent));
        DMLType op = DDL;
        String topic = ddlEvent.getTableId();
        record.put("tableName", topic);
        record.put("data", allData);
        Map<String, Object> header = new HashMap<>();
        header.put("op", op.name());
        record.put("header", header);
        RecordHeaders recordHeaders = new RecordHeaders();
        String key = null;
        Object eventObj = covertData(executeScript(scriptEngine.get(), "process", record, op.name(), null));
        String body = null;
        Integer partition = null;
        if (null == eventObj) {
        } else {
            Map<String, Object> res = (Map<String, Object>) eventObj;
            if (null == res.get("value")) {
                throw new RuntimeException("value cannot be null");
            } else {
                Object obj = res.get("value");
                if (obj instanceof Map) {
                    Map<String, Map<String, Object>> map = (Map<String, Map<String, Object>>) obj;
                    if (map.containsKey("before") && map.get("before").isEmpty()) {
                        map.remove("before");
                    }
                    if (map.containsKey("after") && map.get("after").isEmpty()) {
                        map.remove("after");
                    }
                    res.put("value", map);
                    body = TapSimplify.toJson(obj, JsonParser.ToJsonFeature.WriteMapNullValue);
                } else {
                    body = obj.toString();
                }
            }
            if (res.containsKey("header")) {
                Object obj = res.get("header");
                if (obj instanceof Map) {
                    Map<String, Object> head = (Map<String, Object>) obj;
                    for (String s : head.keySet()) {
                        recordHeaders.add(s, head.get(s).toString().getBytes());
                    }
                } else {
                    throw new RuntimeException("header must be a collection type");
                }
            } else {
                recordHeaders.add("op", op.name().getBytes());
            }
            if (res.containsKey("key")) {
                key = String.valueOf(res.get("key"));
            }
            if (res.containsKey("partition")) {
                partition = Integer.valueOf(String.valueOf(res.get("partition")));
            } else {
                partition = key == null ? null : computePartition(key.getBytes(), kafkaService.getConfig().getNodePartitionSize());
            }
        }
        return new ProducerRecord<>(topic, partition, ddlEvent.getTime(), key, body, recordHeaders);
    }

    @Override
    public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {

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

    public static Object executeScript(ScriptEngine scriptEngine, String function, Object... params) {
        if (scriptEngine != null) {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                return invocable.invokeFunction(function, params);
            } catch (StopException e) {
                throw new RuntimeException(e);
            } catch (ScriptException | NoSuchMethodException | RuntimeException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private Object covertData(Object apply) {
        if (Objects.isNull(apply)) {
            return null;
        } else if (apply instanceof Map) {
            return InstanceFactory.instance(TapUtils.class).cloneMap((Map<String, Object>) apply);//fromJson(toJson(apply));
        } else if (apply instanceof Collection) {
            try {
                return new ArrayList<>((List<Object>) apply);//ConnectorBase.fromJsonArray(toJson(apply));
            } catch (Exception e) {
                String toString = apply.toString();
                if (toString.matches("\\(([0-9]+)\\)\\[.*]")) {
                    toString = toString.replaceFirst("\\(([0-9]+)\\)", "");
                }
                return ConnectorBase.fromJsonArray(toString);
            }
        } else {
            return apply;
        }
    }
}
