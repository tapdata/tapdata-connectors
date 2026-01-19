package io.tapdata.kafka.schema_mode;

import io.tapdata.base.ConnectorBase;
import io.tapdata.constant.DMLType;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
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
import java.util.function.Consumer;

import static io.tapdata.constant.DMLType.*;

public class CustomSchemaMode extends AbsSchemaMode {

    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
    private final ScriptEngine scriptEngine;

    public CustomSchemaMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.CUSTOM, kafkaService);
        try {
            ClassLoader appClassLoader = ScriptOptions.class.getClassLoader();
            scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT,
                    new ScriptOptions().engineName("graal.js").classLoader(appClassLoader));
            String buildInMethod = initBuildInMethod();
            KafkaConfig kafkaConfig = kafkaService.getConfig();
            String script = EmptyKit.isNotBlank(kafkaConfig.getNodeScript()) ? kafkaConfig.getNodeScript() : kafkaConfig.getConnectionScript();
            String scripts = script + System.lineSeparator() + buildInMethod;
            scriptEngine.eval(scripts);
        } catch (Exception e) {
            throw new CoreException("Engine initialization failed!");
        }
    }

    @Override
    public void sampleOneSchema(String table, TapTable sampleTable) {
        kafkaService.<String, String>sampleValue(Collections.singletonList(table), null, record -> {
            if (null != record) {
                Object eventObj = executeScript(scriptEngine, "analyze", record.headers(), record.key(), record.value());
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
            Object value = executeScript(scriptEngine, "analyze", consumerRecord.headers(), consumerRecord.key(), consumerRecord.value());
            Map<String, Object> after = (Map<String, Object>) ((Map<String, Object>) value).get("after");
            Map<String, Object> before = (Map<String, Object>) ((Map<String, Object>) value).get("before");
            String op = String.valueOf(((Map<String, Object>) value).get("op"));
            // 根据操作类型创建对应的 TapEvent
            TapRecordEvent tapEvent;
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
        Object eventObj = covertData(executeScript(scriptEngine, "process", record, op.name(), conditionKeys));
        String body = null;
        if (null == eventObj) {
        } else {
            Map<String, Object> res = (Map<String, Object>) eventObj;
            if (null == res.get("value")) {
                throw new RuntimeException("value cannot be null");
            } else {
                Object obj = res.get("value");
                if (obj instanceof Map) {
                    Map<String, Map<String, Object>> map = (Map<String, Map<String, Object>>) res.get("value");
                    if (map.containsKey("before") && map.get("before").isEmpty()) {
                        map.remove("before");
                    }
                    if (map.containsKey("after") && map.get("after").isEmpty()) {
                        map.remove("after");
                    }
                    res.put("value", map);
                    body = TapSimplify.toJson(res.get("value"), JsonParser.ToJsonFeature.WriteMapNullValue);
                } else {
                    body = obj.toString();
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
                recordHeaders.add("op", op.name().getBytes());
            }
            if (res.containsKey("key")) {
                key = String.valueOf(res.get("key"));
            }
        }
        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(topic,
                null, tapEvent.getTime(), key, body,
                recordHeaders);
        return List.of(producerRecord);
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
//                TapLogger.info(TAG, "Get data and stop script.");
                throw new RuntimeException(e);
            } catch (ScriptException | NoSuchMethodException | RuntimeException e) {
//                TapLogger.error(TAG, "Run script error, message: {}", e.getMessage(), e);
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
