package io.tapdata.kafka.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializeFilter;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.mapping.TapEntry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.value.*;
import io.tapdata.kafka.serialization.json.*;
import io.tapdata.util.JsonSchemaParser;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Kafka 工具类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/2 16:21 Create
 */
public class KafkaUtils {
    private static final JsonSchemaParser SCHEMA_PARSER = new JsonSchemaParser();
    private static final SerializeConfig SERIALIZE_CONFIG = new SerializeConfig();
    private static final Feature[] features = new Feature[]{
//        Feature.AllowISO8601DateFormat,
        Feature.OrderedField,
        Feature.DisableCircularReferenceDetect
    };
    private static final SerializerFeature[] serializerFeatures = new SerializerFeature[]{
//        SerializerFeature.WriteDateUseDateFormat,
        SerializerFeature.WriteMapNullValue
    };

    static {
        SERIALIZE_CONFIG.put(DateTime.class, new DateTimeSerializer());
        SERIALIZE_CONFIG.put(TapDateTimeValue.class, new TapDateTimeValueSerializer());
        SERIALIZE_CONFIG.put(TapDateValue.class, new TapDateValueSerializer());
        SERIALIZE_CONFIG.put(TapTimeValue.class, new TapTimeValueSerializer());
        SERIALIZE_CONFIG.put(TapYearValue.class, new TapYearValueSerializer());
        SERIALIZE_CONFIG.put(byte[].class, new BinarySerializer());
    }

    private KafkaUtils() {
    }

    public static JSONObject parseJsonObject(byte[] bytes) {
        return parseJsonObject(new String(bytes));
    }

    public static JSONObject parseJsonObject(String jsonStr) {
        return JSON.parseObject(jsonStr, features);
    }

    public static <T> T parseObject(byte[] bytes, Class<T> clz) {
        return JSON.parseObject(bytes, clz, features);
    }

    public static <T> List<T> parseList(byte[] bytes, Class<T> clz) {
        return parseList(new String(bytes), clz);
    }

    public static <T> List<T> parseList(String jsonStr, Class<T> clz) {
        return JSON.parseArray(jsonStr, clz);
    }

    public static byte[] toJsonBytes(Object object) {
        return JSON.toJSONBytes(object, SERIALIZE_CONFIG, serializerFeatures);
    }

    public static byte[] toJsonBytes(Object object, SerializeFilter filter) {
        return JSON.toJSONBytes(object, SERIALIZE_CONFIG, filter, serializerFeatures);
    }

    public static void data2TapTableFields(TapTable table, Map<String, Object> data) {
        SCHEMA_PARSER.parse(table, data);
    }


    public static List<TapEntry<String, Function<Object, Object>>> setFieldTypeConvert(TapTable table, List<TapEntry<String, Function<Object, Object>>> converts) {
        table.getNameFieldMap().forEach((k, v) -> {
            if (v.getTapType() instanceof TapDate) {
                converts.add(new TapEntry<>(k, o -> {
                    if (o instanceof String) {
                        return LocalDate.parse((String) o, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay();
                    }
                    return o;
                }));
            }
        });
        return converts;
    }

    public static void convertWithFieldType(List<TapEntry<String, Function<Object, Object>>> converts, TapEvent event) {
        if (event instanceof TapInsertRecordEvent) {
            TapInsertRecordEvent recordEvent = (TapInsertRecordEvent) event;
            Optional.ofNullable(recordEvent.getAfter()).ifPresent(data -> convertWithFieldType(converts, data));
        } else if (event instanceof TapUpdateRecordEvent) {
            TapUpdateRecordEvent recordEvent = (TapUpdateRecordEvent) event;
            Optional.ofNullable(recordEvent.getAfter()).ifPresent(data -> convertWithFieldType(converts, data));
            Optional.ofNullable(recordEvent.getBefore()).ifPresent(data -> convertWithFieldType(converts, data));
        } else if (event instanceof TapDeleteRecordEvent) {
            TapDeleteRecordEvent recordEvent = (TapDeleteRecordEvent) event;
            Optional.ofNullable(recordEvent.getBefore()).ifPresent(data -> convertWithFieldType(converts, data));
        }
    }

    public static void convertWithFieldType(List<TapEntry<String, Function<Object, Object>>> converts, Map<String, Object> data) {
        for (TapEntry<String, Function<Object, Object>> en : converts) {
            Optional.ofNullable(data.get(en.getKey())).ifPresent(v -> data.put(en.getKey(), en.getValue().apply(v)));
        }
    }

    public static DateTime parseDateTime(String dateStr) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
        return new DateTime(localDateTime);
    }
}
