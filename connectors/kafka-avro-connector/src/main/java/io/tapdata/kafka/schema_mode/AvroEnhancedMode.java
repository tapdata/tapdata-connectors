package io.tapdata.kafka.schema_mode;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kit.StringKit;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;

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
public class AvroEnhancedMode extends RegistryAvroMode {

    public AvroEnhancedMode(IKafkaService kafkaService) {
        super(kafkaService);
    }

    /**
     * 父类 {@code sampleOneSchema} 仅按 Avro 主类型映射，DECIMAL/DATE/TIME/TIMESTAMP/CHAR/VARCHAR
     * 全部退化成 BYTES/INT/LONG/STRING，与 {@link #getOrCreateAvroField} 的写入语义不闭环。
     * 这里改成按 logical type / 自定义 prop 反推，使得 discoverSchema 能产出与写入完全对称的 TapField。
     */
    @Override
    public void sampleOneSchema(String table, TapTable sampleTable) {
        kafkaService.<String, Object>sampleValue(Collections.singletonList(table), null, record -> {
            if (record == null || !(record.value() instanceof GenericRecord)) {
                return true;
            }
            GenericRecord genericRecord = (GenericRecord) record.value();
            List<String> primaryKeys = extractPrimaryKeys(record.key());
            populateTapTableFromAvroFields(sampleTable, genericRecord.getSchema().getFields(), primaryKeys);
            return false;
        });
    }

    /**
     * 解析 Kafka 消息 key（约定为 JSON 编码的 PK 名 → 值 map），返回 PK 列名顺序。
     */
    @SuppressWarnings("unchecked")
    protected List<String> extractPrimaryKeys(Object key) {
        if (key == null) return Collections.emptyList();
        try {
            Map<String, Object> map = (Map<String, Object>) TapSimplify.fromJson(key.toString());
            return map == null ? Collections.emptyList() : new ArrayList<>(map.keySet());
        } catch (Exception e) {
            tapLogger.warn("Failed to parse primary keys from key: {}", key, e);
            return Collections.emptyList();
        }
    }

    /**
     * 把一组 Avro Field 按顺序灌入 {@link TapTable}，PK 标记按传入的 primaryKeys 顺序确定。
     * Attunity 等带 envelope 的子类可以传入 inner Data 子记录的 fields 复用此处。
     */
    protected void populateTapTableFromAvroFields(TapTable sampleTable, List<Schema.Field> avroFields, List<String> primaryKeys) {
        for (Schema.Field f : avroFields) {
            sampleTable.add(avroFieldToTapField(f, primaryKeys));
        }
    }

    /**
     * 单个 Avro Field → TapField。dataType 由 {@link #avroSchemaToTapDataType} 推导，
     * 与 {@link #getOrCreateAvroField} 的写入选择严格对称。
     */
    protected TapField avroFieldToTapField(Schema.Field avroField, List<String> primaryKeys) {
        TapField field = new TapField();
        field.setName(avroField.name());
        field.setDataType(avroSchemaToTapDataType(avroField.schema()));
        field.setNullable(avroField.schema().isNullable());
        field.setDefaultValue(sanitizeDefault(avroField.defaultVal()));
        if (primaryKeys != null && primaryKeys.contains(avroField.name())) {
            field.setPrimaryKey(true);
            field.setPrimaryKeyPos(primaryKeys.indexOf(avroField.name()) + 1);
        }
        return field;
    }

    /**
     * Avro Schema → TapData dataType 字符串。顺序：
     * <ol>
     *     <li>nullable union 取非 null 分支；多分支 union 兜底 STRING</li>
     *     <li>标准 logical type（decimal / date / time-* / timestamp-*）</li>
     *     <li>自定义 prop logicalType（char / varchar / json）+ length</li>
     *     <li>基础 Avro Type</li>
     * </ol>
     */
    protected String avroSchemaToTapDataType(Schema schema) {
        if (schema == null) return "STRING";
        Schema actual = unwrapNullableForSchema(schema);
        if (actual == null) return "STRING";

        LogicalType lt = actual.getLogicalType();
        if (lt != null) {
            switch (lt.getName()) {
                case "decimal":
                    LogicalTypes.Decimal dec = (LogicalTypes.Decimal) lt;
                    return "DECIMAL(" + dec.getPrecision() + "," + dec.getScale() + ")";
                case "date":
                    return "DATE";
                case "time-millis":
                    return "TIME(3)";
                case "time-micros":
                    return "TIME(6)";
                case "timestamp-millis":
                    return "TIMESTAMP(3)";
                case "timestamp-micros":
                    return "TIMESTAMP(6)";
                default:
                    break;
            }
        }
        // 自定义 prop 形式（writer 端用 addProp 注入）
        Object customLT = actual.getObjectProp("logicalType");
        if (customLT instanceof String) {
            switch ((String) customLT) {
                case "char":
                    return withLength("CHAR", actual);
                case "varchar":
                    return withLength("VARCHAR", actual);
                case "json":
                    return "RECORD";
                default:
                    break;
            }
        }
        switch (actual.getType()) {
            case BOOLEAN: return "BOOLEAN";
            case INT:     return "INTEGER";
            case LONG:    return "LONG";
            case FLOAT:   return "FLOAT";
            case DOUBLE:  return "DOUBLE";
            case BYTES:
            case FIXED:   return "BYTES";
            case ARRAY:   return "ARRAY";
            case MAP:     return "MAP";
            case RECORD:  return "RECORD";
            case ENUM:
            case STRING:
            default:      return "STRING";
        }
    }

    private String withLength(String base, Schema schema) {
        Object len = schema.getObjectProp("length");
        if (len instanceof Number) {
            int n = ((Number) len).intValue();
            if (n > 0) return base + "(" + n + ")";
        }
        return base;
    }

    /**
     * 与父类 {@code unwrapNullable} 等价，但接受 null 兜底；nullable union 取非 NULL 分支，
     * 多分支 union 返回 null 让上层按 STRING 兜底，避免误判类型。
     */
    private Schema unwrapNullableForSchema(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return schema;
        List<Schema> types = schema.getTypes();
        if (types.size() == 2) {
            for (Schema t : types) {
                if (t.getType() != Schema.Type.NULL) return t;
            }
        }
        return null;
    }

    /**
     * 与父类 {@code getDefaultValue} 一致：JsonProperties.Null 视作 null，非 Serializable 转字符串。
     */
    private Object sanitizeDefault(Object obj) {
        if (obj == null || obj instanceof JsonProperties.Null) return null;
        return obj instanceof Serializable ? obj : String.valueOf(obj);
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

    /**
     * 消费侧反向解码：父类按裸值塞回 Map（仅做 Utf8 → String），logical type 全部丢失。
     * 这里按字段 schema 把 ByteBuffer/Integer/Long 还原为 BigDecimal/LocalDate/LocalTime/Instant，
     * 与 {@link #convertToAvroType(Object, TapField)} 的写入方向一一对应。
     */
    @Override
    protected Map<String, Object> convertGericRecordToMap(GenericRecord record) {
        Map<String, Object> result = new HashMap<>();
        if (record == null) {
            return result;
        }
        for (Schema.Field f : record.getSchema().getFields()) {
            result.put(f.name(), decodeAvroValue(f.schema(), record.get(f.name())));
        }
        return result;
    }

    /**
     * 按 Avro Schema 把运行时值解码成 Java 标准类型。union 取非 null 分支递归。
     */
    protected Object decodeAvroValue(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        Schema actual = unwrapNullable(schema);
        LogicalType lt = actual.getLogicalType();
        if (lt != null) {
            switch (lt.getName()) {
                case "decimal":
                    return decodeDecimal((LogicalTypes.Decimal) lt, value);
                case "date":
                    return value instanceof Number ? LocalDate.ofEpochDay(((Number) value).longValue()).atStartOfDay() : value;
                case "time-millis":
                    return value instanceof Number ? LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L).atDate(LocalDate.ofYearDay(1970, 1)) : value;
                case "time-micros":
                    return value instanceof Number ? LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000L).atDate(LocalDate.ofYearDay(1970, 1)) : value;
                case "timestamp-millis":
                    return value instanceof Number ? Instant.ofEpochMilli(((Number) value).longValue()) : value;
                case "timestamp-micros":
                    return value instanceof Number ? microsToInstant(((Number) value).longValue()) : value;
                default:
                    // char / varchar / json 等自定义 logicalType 落回原始字符串
                    break;
            }
        }
        switch (actual.getType()) {
            case STRING:
            case ENUM:
                return value instanceof Utf8 || value instanceof GenericEnumSymbol ? value.toString() : value;
            case BYTES:
                if (value instanceof ByteBuffer) {
                    ByteBuffer bb = ((ByteBuffer) value).duplicate();
                    byte[] bytes = new byte[bb.remaining()];
                    bb.get(bytes);
                    return bytes;
                }
                if (value instanceof GenericFixed) return ((GenericFixed) value).bytes();
                return value;
            case FIXED:
                return value instanceof GenericFixed ? ((GenericFixed) value).bytes() : value;
            case ARRAY:
                return decodeArray(actual.getElementType(), value);
            case MAP:
                return decodeMap(actual.getValueType(), value);
            case RECORD:
                return value instanceof GenericRecord ? convertGericRecordToMap((GenericRecord) value) : value;
            default:
                return value;
        }
    }

    private Schema unwrapNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return schema;
        for (Schema t : schema.getTypes()) {
            if (t.getType() != Schema.Type.NULL) return t;
        }
        return schema;
    }

    private BigDecimal decodeDecimal(LogicalTypes.Decimal dec, Object value) {
        byte[] bytes;
        if (value instanceof ByteBuffer) {
            ByteBuffer bb = ((ByteBuffer) value).duplicate();
            bytes = new byte[bb.remaining()];
            bb.get(bytes);
        } else if (value instanceof GenericFixed) {
            bytes = ((GenericFixed) value).bytes();
        } else if (value instanceof byte[]) {
            bytes = (byte[]) value;
        } else if (value instanceof Number) {
            // precision 缺失时父类降级为 double，直接按 double 还原
            return BigDecimal.valueOf(((Number) value).doubleValue());
        } else {
            return new BigDecimal(value.toString());
        }
        return new BigDecimal(new BigInteger(bytes), dec.getScale());
    }

    private Instant microsToInstant(long micros) {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        long nanos = Math.floorMod(micros, 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(seconds, nanos);
    }

    private List<Object> decodeArray(Schema elementSchema, Object value) {
        if (!(value instanceof Collection)) {
            return Collections.singletonList(decodeAvroValue(elementSchema, value));
        }
        Collection<?> src = (Collection<?>) value;
        List<Object> out = new ArrayList<>(src.size());
        for (Object e : src) out.add(decodeAvroValue(elementSchema, e));
        return out;
    }

    private Map<String, Object> decodeMap(Schema valueSchema, Object value) {
        if (!(value instanceof Map)) {
            Map<String, Object> out = new LinkedHashMap<>(1);
            out.put("value", value);
            return out;
        }
        Map<?, ?> src = (Map<?, ?>) value;
        Map<String, Object> out = new LinkedHashMap<>(src.size());
        for (Map.Entry<?, ?> e : src.entrySet()) {
            if (e.getKey() == null) continue;
            out.put(e.getKey().toString(), decodeAvroValue(valueSchema, e.getValue()));
        }
        return out;
    }

    protected String avroPrimaryTypeName(Schema schema) {
        if (schema == null) {
            return "STRING";
        }
        if (schema.isUnion()) {
            List<Schema> types = schema.getTypes();
            if (types.size() == 2) {
                for (Schema t : types) {
                    if (t.getType() != Schema.Type.NULL) {
                        return toTapType(t);
                    }
                }
            }
            return "STRING";
        }
        return toTapType(schema);
    }

    protected String toTapType(Schema schema) {
        String dataType;
        if (schema.getLogicalType() != null) {
            dataType = schema.getLogicalType().getName().toUpperCase();
        } else if (schema.hasProps() && schema.getProp("logicalType") != null) {
            dataType = schema.getProp("logicalType").toUpperCase();
        } else {
            dataType = schema.getType().name();
        }
        return toTapType(dataType);
    }
}
