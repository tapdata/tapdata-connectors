package io.tapdata.kafka.schema_mode;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kit.StringKit;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link RegistryAvroMode} 的扩展范例：在父类生成的事件之上，把 Kafka 消息的元数据
 * （headers / partition / offset / timestamp）附加到 {@link TapEvent#getInfo()}，
 * 供下游做溯源或自定义路由。父类已实现的 schema diff、DDL 检测、主键 DDL 过滤等行为全部保留。
 * <p>
 * 作用域仅限本连接器实例，不影响同 JVM 内的其他 Kafka 连接器。
 */
public class AvroEnhanceMode extends RegistryAvroMode {

    public AvroEnhanceMode(IKafkaService kafkaService) {
        super(kafkaService);
    }

    protected Schema.Field getOrCreateAvroField(TapTable tapTable, TapField tapField) {
        final String columnName = tapField.getName();
        if (fieldCache.containsKey(tapTable.getId() + "." + columnName)) {
            return fieldCache.get(tapTable.getId() + "." + columnName);
        }

        final String columnType = StringKit.removeParentheses(tapField.getDataType());
        boolean nullable = !Boolean.FALSE.equals(tapField.getNullable());

        // 根据类型创建基础 Schema，必要时附加 logicalType
        Schema baseType;
        switch (columnType) {
            case "BOOLEAN":
                baseType = SchemaBuilder.builder().booleanType();
                break;
            case "INTEGER":
                baseType = SchemaBuilder.builder().intType();
                break;
            case "LONG":
                baseType = SchemaBuilder.builder().longType();
                break;
            case "FLOAT":
                baseType = SchemaBuilder.builder().floatType();
                break;
            case "DOUBLE":
                baseType = SchemaBuilder.builder().doubleType();
                break;
            case "DECIMAL":
                baseType = decimalSchema(tapField);
                break;
            case "DATE":
                baseType = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
                break;
            case "TIME":
                baseType = timeSchema(tapField);
                break;
            case "TIMESTAMP":
                baseType = timestampSchema(tapField);
                break;
            case "BYTES":
                baseType = SchemaBuilder.builder().bytesType();
                break;
            case "CHAR":
                baseType = charSchema(tapField);
                break;
            case "VARCHAR":
                baseType = varcharSchema(tapField);
                break;
            case "ARRAY":
                baseType = arraySchema();
                break;
            case "MAP":
                baseType = mapSchema();
                break;
            case "RECORD":
                baseType = recordSchema();
                break;
            case "STRING":
            default:
                baseType = SchemaBuilder.builder().stringType();
                break;
        }

        // nullable -> union [null, type]
        Schema avroType = nullable
                ? Schema.createUnion(Schema.create(Schema.Type.NULL), baseType)
                : baseType;

        Schema.Field field;
        if (applyDefault) {
            field = new Schema.Field(columnName, avroType, null, tapField.getDefaultValue());
        } else {
            field = new Schema.Field(columnName, avroType, null, null);
        }
        fieldCache.put(tapTable.getId() + "." + columnName, field);
        return field;
    }

    /**
     * NUMBER -> bytes + decimal(precision, scale)。
     * 若 precision 缺失或不合法，退化为 double，避免 LogicalTypes.decimal 校验失败。
     */
    private Schema decimalSchema(TapField tapField) {
        Pair<Integer, Integer> precisionScale = getFieldPrecisionAndScale(tapField.getDataType());
        Integer precision = precisionScale.getLeft();
        Integer scale = precisionScale.getRight();
        if (precision == null || precision <= 0) {
            return SchemaBuilder.builder().doubleType();
        }
        int s = scale == null || scale < 0 ? 0 : scale;
        if (s > precision) {
            s = precision;
        }
        return LogicalTypes.decimal(precision, s).addToSchema(SchemaBuilder.builder().bytesType());
    }

    /**
     * TIME -> int + time-millis；scale >= 4 时使用 long + time-micros 保留亚毫秒精度。
     */
    private Schema timeSchema(TapField tapField) {
        Integer scale = getFieldFraction(tapField.getDataType());
        if (scale != null && scale >= 4) {
            return LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType());
        }
        return LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
    }

    /**
     * DATETIME / TIMESTAMP -> long + timestamp-millis；scale >= 4 时升级为 timestamp-micros。
     */
    private Schema timestampSchema(TapField tapField) {
        Integer scale = getFieldFraction(tapField.getDataType());
        if (scale != null && scale >= 4) {
            return LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType());
        }
        return LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());
    }

    /**
     * CHAR -> string，自定义 logicalType=char，并附 length，便于下游还原定长语义。
     */
    private Schema charSchema(TapField tapField) {
        Schema schema = SchemaBuilder.builder().stringType();
        schema.addProp("logicalType", "char");
        Integer length = resolveLength(tapField);
        if (length != null && length > 0) {
            schema.addProp("length", length);
        }
        return schema;
    }

    /**
     * VARCHAR -> string，自定义 logicalType=varchar，并附 maxLength。
     */
    private Schema varcharSchema(TapField tapField) {
        Schema schema = SchemaBuilder.builder().stringType();
        schema.addProp("logicalType", "varchar");
        Integer length = resolveLength(tapField);
        if (length != null && length > 0) {
            schema.addProp("length", length);
        }
        return schema;
    }

    /**
     * ARRAY -> Avro array。元素类型未知，按 nullable string 兜底，保持与父类 stringify 写入一致。
     */
    private Schema arraySchema() {
        Schema element = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        return Schema.createArray(element);
    }

    /**
     * MAP -> Avro map（key 固定 string）。value 类型未知，按 nullable string 兜底。
     */
    private Schema mapSchema() {
        Schema value = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        return Schema.createMap(value);
    }

    /**
     * RECORD -> string + logicalType=json。嵌套结构在 TapField 中不可见，按 JSON 编码兜底。
     */
    private Schema recordSchema() {
        Schema schema = SchemaBuilder.builder().stringType();
        schema.addProp("logicalType", "json");
        return schema;
    }

    /**
     * 优先取 TapField.length；缺失时从 dataType 中解析括号里的第一个数字。
     */
    private Integer resolveLength(TapField tapField) {
        Integer length = tapField.getLength();
        if (length != null && length > 0) {
            return length;
        }
        return getFieldLength(tapField.getDataType());
    }

    public Integer getFieldLength(String dataType) {
        //提取括号里的值
        Pattern pattern = Pattern.compile("\\(([^)]+)\\)");
        Matcher matcher = pattern.matcher(dataType);
        if (matcher.find()) {
            long length = Long.parseLong(matcher.group(1));
            if (length > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            } else {
                return (int) length;
            }
        }
        return Integer.MAX_VALUE;
    }

    public Integer getFieldFraction(String dataType) {
        //提取括号里的值
        Pattern pattern = Pattern.compile("\\(([^)]+)\\)");
        Matcher matcher = pattern.matcher(dataType);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return 6;
    }

    public Pair<Integer, Integer> getFieldPrecisionAndScale(String dataType) {
        //提取括号里的值,逗号的前一个和后一个
        Pattern pattern = Pattern.compile("\\(([^,]+),([^)]+)\\)");
        Matcher matcher = pattern.matcher(dataType);
        if (matcher.find()) {
            return Pair.of(Integer.parseInt(matcher.group(1).trim()), Integer.parseInt(matcher.group(2).trim()));
        }
        return Pair.of(38, 10);
    }

    /**
     * 与 {@link #getOrCreateAvroField} 的 logical type 选择保持一一对应。
     * 父类 {@code convertToAvroType(Object, String)} 在 DECIMAL/DATE/TIME/TIMESTAMP/BYTES/ARRAY/MAP/RECORD
     * 上会退化为字符串，与新 schema 不兼容；此处按 logical type 的运行时表示重新转换。
     */
    protected Object convertToAvroType(Object value, TapField tapField) {
        if (value == null) {
            return null;
        }
        String dataType = StringKit.removeParentheses(tapField.getDataType());
        switch (dataType) {
            case "DECIMAL":
                return toDecimal(value, tapField);
            case "DATE":
                return toEpochDays(value);
            case "TIME":
                return toTimeOfDay(value, tapField);
            case "TIMESTAMP":
            case "DATETIME":
                return toEpochInstant(value, tapField);
            case "BYTES":
                return toByteBuffer(value);
            case "ARRAY":
                return toAvroArray(value);
            case "MAP":
                return toAvroMap(value);
            case "RECORD":
                return value instanceof String ? value : JSON.toJSONString(value);
            default:
                return super.convertToAvroType(value, dataType);
        }
    }

    private Object toDecimal(Object value, TapField tapField) {
        Pair<Integer, Integer> ps = getFieldPrecisionAndScale(tapField.getDataType());
        Integer precision = ps.getLeft();
        // schema 在 precision 缺失时退化为 double，conversion 必须对齐
        if (precision == null || precision <= 0) {
            if (value instanceof Number) return ((Number) value).doubleValue();
            return Double.parseDouble(value.toString());
        }
        int scale = ps.getRight() == null || ps.getRight() < 0 ? 0 : ps.getRight();
        if (scale > precision) scale = precision;
        BigDecimal bd;
        if (value instanceof BigDecimal) {
            bd = (BigDecimal) value;
        } else if (value instanceof BigInteger) {
            bd = new BigDecimal((BigInteger) value);
        } else if (value instanceof Number) {
            bd = BigDecimal.valueOf(((Number) value).doubleValue());
        } else {
            bd = new BigDecimal(value.toString());
        }
        BigInteger unscaled = bd.setScale(scale, RoundingMode.HALF_UP).unscaledValue();
        return ByteBuffer.wrap(unscaled.toByteArray());
    }

    private Integer toEpochDays(Object value) {
        return (int) ((LocalDate) value).toEpochDay();
    }

    private Object toTimeOfDay(Object value, TapField tapField) {
        Integer scale = getFieldFraction(tapField.getDataType());
        boolean micros = scale != null && scale >= 4;
        long nanoOfDay = ((LocalTime) value).toNanoOfDay();
        // 不能用三元: int/long 混用会被提升为 long 后装箱成 Long，破坏 time-millis (Integer) 类型契约
        if (micros) {
            return nanoOfDay / 1_000L;
        }
        return (int) (nanoOfDay / 1_000_000L);
    }

    private Long toEpochInstant(Object value, TapField tapField) {
        Integer scale = getFieldFraction(tapField.getDataType());
        boolean micros = scale != null && scale >= 4;
        Instant instant = (Instant) value;
        return micros
                ? Math.multiplyExact(instant.getEpochSecond(), 1_000_000L) + instant.getNano() / 1_000L
                : instant.toEpochMilli();
    }

    private ByteBuffer toByteBuffer(Object value) {
        if (value instanceof ByteBuffer) return (ByteBuffer) value;
        if (value instanceof byte[]) return ByteBuffer.wrap((byte[]) value);
        return ByteBuffer.wrap(value.toString().getBytes(StandardCharsets.UTF_8));
    }

    private List<Object> toAvroArray(Object value) {
        if (value instanceof Collection) {
            Collection<?> src = (Collection<?>) value;
            List<Object> out = new ArrayList<>(src.size());
            for (Object e : src) out.add(e == null ? null : e.toString());
            return out;
        }
        List<Object> out = new ArrayList<>(1);
        out.add(value.toString());
        return out;
    }

    private Map<String, Object> toAvroMap(Object value) {
        if (value instanceof Map) {
            Map<?, ?> src = (Map<?, ?>) value;
            Map<String, Object> out = new LinkedHashMap<>(src.size());
            for (Map.Entry<?, ?> e : src.entrySet()) {
                if (e.getKey() == null) continue;
                out.put(e.getKey().toString(), e.getValue() == null ? null : e.getValue().toString());
            }
            return out;
        }
        Map<String, Object> out = new LinkedHashMap<>(1);
        out.put("value", value.toString());
        return out;
    }
}
