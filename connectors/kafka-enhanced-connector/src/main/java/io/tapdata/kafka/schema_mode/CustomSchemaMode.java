package io.tapdata.kafka.schema_mode;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.exception.StopException;
import io.tapdata.kafka.AbsSchemaMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class CustomSchemaMode extends AbsSchemaMode {

    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");

    public CustomSchemaMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.CUSTOM, kafkaService);
    }

    @Override
    public void sampleOneSchema(String table, TapTable sampleTable) {
        ScriptEngine scriptEngine;
        try {
            ClassLoader appClassLoader = ScriptOptions.class.getClassLoader();
            scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT,
                    new ScriptOptions().engineName("graal.js").classLoader(appClassLoader));
            String buildInMethod = initBuildInMethod();
            String scripts = getKafkaService().getConfig().getConnectionScript() + System.lineSeparator() + buildInMethod;
            scriptEngine.eval(scripts);
        } catch (Exception e) {
            throw new CoreException("Engine initialization failed!");
        }
        kafkaService.<String, String>sampleValue(Collections.singletonList(table), null, record -> {
            if (null != record) {
                Object eventObj = executeScript(scriptEngine, "analyze", record.headers(), record.key(), record.value());
                if (eventObj instanceof Map) {
                    for (Map.Entry<String, Object> entry : ((Map<String, Object>) eventObj).entrySet()) {
                        String fieldName = entry.getKey();
                        Object fieldValue = entry.getValue();
                        TapField tapField = new TapField(fieldName, inferTapType(fieldValue));
                        sampleTable.add(tapField);
                    }
                }
                return false;
            }
            return true;
        });
    }

    @Override
    public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
        return null;
    }

    @Override
    public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable table, TapEvent tapEvent) {
        return List.of();
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
}
