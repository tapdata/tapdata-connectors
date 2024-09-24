package io.tapdata.kafka.constants;

/**
 * Kafka 序列化工具类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/29 18:44 Create
 */
public enum KafkaSerialization {

    STANDARD("Standard", "MAP", true),
    JSON_ARRAY("JsonArray", "ARRAY", true),
    JSON_OBJECT("JsonObject", "MAP", true),
    BYTE_ARRAY("ByteArray", "BINARY"),
    STRING("String", "STRING"),
//    TEXT("Text", "TEXT", "String"),
//    ARRAY("Array", "ARRAY", "ByteArray"),
//    MAP("Map", "MAP", "ByteArray"),
    SHORT("Short", "SHORT"),
    INTEGER("Integer", "INTEGER"),
    LONG("Long", "LONG"),
    UUID("UUID", "UUID"),
    ;

    private final String type;
    private final String dataType;
    private final String serializer;
    private final String deserializer;

    KafkaSerialization(String type, String dataType) {
        this(type, dataType, false);
    }

    KafkaSerialization(String type, String dataType, boolean custom) {
        this(type, dataType, custom, type);
    }

    KafkaSerialization(String type, String dataType, boolean custom, String serialization) {
        this(type, dataType
            , custom ? String.format("io.tapdata.kafka.serialization.%sSerializer", serialization) : String.format("org.apache.kafka.common.serialization.%sSerializer", serialization)
            , custom ? String.format("io.tapdata.kafka.serialization.%sDeserializer", serialization) : String.format("org.apache.kafka.common.serialization.%sDeserializer", serialization)
        );
    }

    KafkaSerialization(String type, String dataType, String serializer, String deserializer) {
        this.type = type;
        this.dataType = dataType;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public String getType() {
        return type;
    }

    public String getDataType() {
        return dataType;
    }

    public String getSerializer() {
        return serializer;
    }

    public String getDeserializer() {
        return deserializer;
    }

    public static KafkaSerialization fromString(String serialization) {
        for (KafkaSerialization ks : KafkaSerialization.values()) {
            if (ks.getType().equals(serialization)) {
                return ks;
            }
        }
        return null;
    }
}
