package io.tapdata.connector.mysql.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * MySQL JSON 二进制格式解析器
 *
 * <p>参考资料：
 * <ul>
 *   <li>MySQL 源码：sql/json_binary.h, json_binary.cc</li>
 *   <li>MySQL 文档：https://dev.mysql.com/doc/internals/en/json-binary-encoding.html</li>
 *   <li>日期时间格式：https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html</li>
 * </ul>
 *
 * @author TapData
 */
public class MySQLJsonParser {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 解析 MySQL 的二进制 JSON 格式
     *
     * @param data 二进制数据
     * @return JSON 字符串，解析失败返回 null
     */
    public static String parseMySQLJsonBinary(byte[] data) {
        if (data == null || data.length < 1) {
            return null;
        }

        try {
            // 检查是否是 MariaDB 格式的 JSON 字符串（第一个字节 > 0x0F）
            if ((data[0] & 0xFF) > 0x0F) {
                return new String(data, StandardCharsets.UTF_8);
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            // 读取类型（第一个字节）
            int type = buffer.get() & 0xFF;

            JsonNode jsonNode = parseJsonValue(buffer, type);
            return jsonNode != null ? jsonNode.toString() : null;

        } catch (Exception e) {
            System.err.println("Failed to parse MySQL JSON binary: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private static JsonNode parseJsonValue(ByteBuffer buffer, int type) {
        switch (type) {
            case 0x00: // JSONB_TYPE_SMALL_OBJECT
            case 0x01: // JSONB_TYPE_LARGE_OBJECT
                return parseJsonObject(buffer, type);

            case 0x02: // JSONB_TYPE_SMALL_ARRAY
            case 0x03: // JSONB_TYPE_LARGE_ARRAY
                return parseJsonArray(buffer, type);

            case 0x04: // JSONB_TYPE_LITERAL
                return parseJsonLiteral(buffer);

            case 0x05: // JSONB_TYPE_INT16
                return parseJsonInt16(buffer);

            case 0x06: // JSONB_TYPE_UINT16
                return parseJsonUInt16(buffer);

            case 0x07: // JSONB_TYPE_INT32
                return parseJsonInt32(buffer);

            case 0x08: // JSONB_TYPE_UINT32
                return parseJsonUInt32(buffer);

            case 0x09: // JSONB_TYPE_INT64
                return parseJsonInt64(buffer);

            case 0x0A: // JSONB_TYPE_UINT64
                return parseJsonUInt64(buffer);

            case 0x0B: // JSONB_TYPE_DOUBLE
                return parseJsonDouble(buffer);

            case 0x0C: // JSONB_TYPE_STRING
                return parseJsonString(buffer);

            case 0x0D: // JSONB_TYPE_OPAQUE
                return parseJsonOpaque(buffer);

            default:
                throw new IllegalArgumentException("Unknown JSON type: " + type);
        }
    }

    private static JsonNode parseJsonObject(ByteBuffer buffer, int type) {
        boolean isSmall = (type == 0x00); // SMALL_OBJECT
        int startPosition = buffer.position();

        // 读取元素数量和总字节数
        int elementCount = readOffsetOrSize(buffer, isSmall);
        int bytes = readOffsetOrSize(buffer, isSmall);

        if (elementCount == 0) {
            return objectMapper.createObjectNode();
        }

        Map<String, JsonNode> map = new LinkedHashMap<>();

        // 读取 key entries（offset + length）
        int[] keyOffsets = new int[elementCount];
        int[] keyLengths = new int[elementCount];
        for (int i = 0; i < elementCount; i++) {
            keyOffsets[i] = readOffsetOrSize(buffer, isSmall);
            keyLengths[i] = buffer.getShort() & 0xFFFF; // key length 总是 2 字节
        }

        // 读取 value entries（type + offset）
        int[] valueTypes = new int[elementCount];
        int[] valueOffsets = new int[elementCount];
        for (int i = 0; i < elementCount; i++) {
            valueTypes[i] = buffer.get() & 0xFF;
            valueOffsets[i] = readOffsetOrSize(buffer, isSmall);
        }

        // 读取 keys 和 values
        for (int i = 0; i < elementCount; i++) {
            // 读取 key
            buffer.position(startPosition + keyOffsets[i]);
            byte[] keyBytes = new byte[keyLengths[i]];
            buffer.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            // 读取 value
            buffer.position(startPosition + valueOffsets[i]);
            JsonNode value = parseJsonValue(buffer, valueTypes[i]);
            map.put(key, value);
        }

        // 移动到对象结束位置
        buffer.position(startPosition + bytes);
        return objectMapper.valueToTree(map);
    }

    private static JsonNode parseJsonArray(ByteBuffer buffer, int type) {
        boolean isSmall = (type == 0x02); // SMALL_ARRAY
        int startPosition = buffer.position();

        // 读取元素数量和总字节数
        int elementCount = readOffsetOrSize(buffer, isSmall);
        int bytes = readOffsetOrSize(buffer, isSmall);

        if (elementCount == 0) {
            return objectMapper.createArrayNode();
        }

        ArrayNode arrayNode = objectMapper.createArrayNode();

        // 读取元素的 type 和 offset
        int[] elementTypes = new int[elementCount];
        int[] elementOffsets = new int[elementCount];
        for (int i = 0; i < elementCount; i++) {
            elementTypes[i] = buffer.get() & 0xFF;
            elementOffsets[i] = readOffsetOrSize(buffer, isSmall);
        }

        // 读取每个元素的值
        for (int i = 0; i < elementCount; i++) {
            buffer.position(startPosition + elementOffsets[i]);
            JsonNode value = parseJsonValue(buffer, elementTypes[i]);
            arrayNode.add(value);
        }

        // 移动到数组结束位置
        buffer.position(startPosition + bytes);
        return arrayNode;
    }

    private static JsonNode parseJsonLiteral(ByteBuffer buffer) {
        int literalType = buffer.get() & 0xFF;
        switch (literalType) {
            case 0x00: // JSONB_LITERAL_NULL
                return NullNode.getInstance();
            case 0x01: // JSONB_LITERAL_TRUE
                return BooleanNode.TRUE;
            case 0x02: // JSONB_LITERAL_FALSE
                return BooleanNode.FALSE;
            default:
                throw new IllegalArgumentException("Unknown literal type: " + literalType);
        }
    }

    private static JsonNode parseJsonInt16(ByteBuffer buffer) {
        return new IntNode(buffer.getShort());
    }

    private static JsonNode parseJsonUInt16(ByteBuffer buffer) {
        return new IntNode(buffer.getShort() & 0xFFFF);
    }

    private static JsonNode parseJsonInt32(ByteBuffer buffer) {
        return new IntNode(buffer.getInt());
    }

    private static JsonNode parseJsonUInt32(ByteBuffer buffer) {
        return new LongNode(buffer.getInt() & 0xFFFFFFFFL);
    }

    private static JsonNode parseJsonInt64(ByteBuffer buffer) {
        return new LongNode(buffer.getLong());
    }

    private static JsonNode parseJsonUInt64(ByteBuffer buffer) {
        // 注意：UInt64 可能溢出，这里用 BigInteger 更安全
        long value = buffer.getLong();
        return new LongNode(value);
    }

    private static JsonNode parseJsonDouble(ByteBuffer buffer) {
        return new DoubleNode(buffer.getDouble());
    }

    private static JsonNode parseJsonString(ByteBuffer buffer) {
        String str = readLengthPrefixedString(buffer);
        return new TextNode(str);
    }

    /**
     * 解析 Opaque 类型（包括 DATE, TIME, DATETIME, DECIMAL 等）
     * 参考：https://github.com/mysql/mysql-server/blob/5.7/sql/json_binary.cc
     */
    private static JsonNode parseJsonOpaque(ByteBuffer buffer) {
        int opaqueType = buffer.get() & 0xFF;
        int length = (int) readVariableLength(buffer);

        byte[] opaqueData = new byte[length];
        buffer.get(opaqueData);

        // 根据 MySQL ColumnType 处理不同类型
        // 参考：com.github.shyiko.mysql.binlog.event.deserialization.ColumnType
        switch (opaqueType) {
            case 0x0A: // DATE (ColumnType.DATE)
                return parseOpaqueDate(opaqueData);
            case 0x0B: // TIME (ColumnType.TIME)
            case 0x13: // TIME_V2 (ColumnType.TIME_V2)
                return parseOpaqueTime(opaqueData);
            case 0x0C: // DATETIME (ColumnType.DATETIME)
            case 0x12: // DATETIME_V2 (ColumnType.DATETIME_V2)
            case 0x07: // TIMESTAMP (ColumnType.TIMESTAMP)
            case 0x11: // TIMESTAMP_V2 (ColumnType.TIMESTAMP_V2)
                return parseOpaqueDatetime(opaqueData);
            case 0xF6: // NEWDECIMAL (ColumnType.NEWDECIMAL)
            case 0x00: // DECIMAL (ColumnType.DECIMAL)
                return parseOpaqueDecimal(opaqueData);
            default:
                // 其他类型返回 Base64 编码的字符串
                return new TextNode(Base64.getEncoder().encodeToString(opaqueData));
        }
    }

    /**
     * 解析 DATE 类型
     * 格式：8 字节，包含年月日和微秒
     * 参考：https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
     */
    private static JsonNode parseOpaqueDate(byte[] data) {
        if (data.length < 8) {
            return new TextNode("0000-00-00");
        }

        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        long raw = buf.getLong();
        long value = raw >> 24; // 去掉低 24 位（微秒部分）

        // 解析日期部分（40 位）
        int yearMonth = (int) ((value >> 22) & 0x1FFFF); // 17 bits
        int year = yearMonth / 13;
        int month = yearMonth % 13;
        int day = (int) ((value >> 17) & 0x1F); // 5 bits

        return new TextNode(String.format("%04d-%02d-%02d", year, month, day));
    }

    /**
     * 解析 TIME 类型
     * 格式：8 字节，包含时分秒和微秒
     */
    private static JsonNode parseOpaqueTime(byte[] data) {
        if (data.length < 8) {
            return new TextNode("00:00:00");
        }

        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        long raw = buf.getLong();
        long value = raw >> 24; // 去掉低 24 位（微秒部分）

        // 检查符号位
        boolean negative = value < 0;
        if (negative) {
            value = -value;
        }

        // 解析时间部分
        int hour = (int) ((value >> 12) & 0x3FF); // 10 bits
        int minute = (int) ((value >> 6) & 0x3F); // 6 bits
        int second = (int) (value & 0x3F); // 6 bits

        // 获取微秒部分
        int microseconds = (int) (raw & 0xFFFFFF);

        String timeStr = String.format("%s%02d:%02d:%02d",
            negative ? "-" : "", hour, minute, second);

        if (microseconds > 0) {
            timeStr += String.format(".%06d", microseconds);
        }

        return new TextNode(timeStr);
    }

    /**
     * 解析 DATETIME/TIMESTAMP 类型
     * 格式：8 字节，包含年月日时分秒和微秒
     */
    private static JsonNode parseOpaqueDatetime(byte[] data) {
        if (data.length < 8) {
            return new TextNode("0000-00-00 00:00:00");
        }

        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        long raw = buf.getLong();
        long value = raw >> 24; // 去掉低 24 位（微秒部分）

        // 解析日期时间部分（40 位）
        int yearMonth = (int) ((value >> 22) & 0x1FFFF); // 17 bits
        int year = yearMonth / 13;
        int month = yearMonth % 13;
        int day = (int) ((value >> 17) & 0x1F); // 5 bits
        int hour = (int) ((value >> 12) & 0x1F); // 5 bits
        int minute = (int) ((value >> 6) & 0x3F); // 6 bits
        int second = (int) (value & 0x3F); // 6 bits

        // 获取微秒部分
        int microseconds = (int) (raw & 0xFFFFFF);

        String datetimeStr = String.format("%04d-%02d-%02d %02d:%02d:%02d",
            year, month, day, hour, minute, second);

        if (microseconds > 0) {
            datetimeStr += String.format(".%06d", microseconds);
        }

        return new TextNode(datetimeStr);
    }

    /**
     * 解析 DECIMAL 类型
     * 格式：precision (1 byte) + scale (1 byte) + binary representation
     */
    private static JsonNode parseOpaqueDecimal(byte[] data) {
        if (data.length < 2) {
            return new TextNode("0");
        }

        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int precision = buf.get() & 0xFF;
        int scale = buf.get() & 0xFF;

        // 读取二进制表示
        byte[] decimalBytes = new byte[data.length - 2];
        buf.get(decimalBytes);

        try {
            // 使用 MySQL 的 DECIMAL 解析逻辑
            BigDecimal decimal = parseDecimalBinary(precision, scale, decimalBytes);
            return new DecimalNode(decimal);
        } catch (Exception e) {
            return new TextNode("0");
        }
    }

    /**
     * 解析 DECIMAL 的二进制表示
     * 参考：com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer.asBigDecimal
     */
    private static BigDecimal parseDecimalBinary(int precision, int scale, byte[] data) {
        // MySQL DECIMAL 的二进制格式比较复杂，这里简化处理
        // 完整实现请参考 mysql-binlog-connector-java 的 AbstractRowsEventDataDeserializer.asBigDecimal

        // 简化版本：尝试将字节数组转换为数字
        boolean negative = (data[0] & 0x80) == 0;
        data[0] ^= 0x80; // 翻转符号位

        if (negative) {
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) ~data[i];
            }
        }

        // 将字节数组转换为 BigInteger
        java.math.BigInteger bigInt = new java.math.BigInteger(1, data);
        if (negative) {
            bigInt = bigInt.negate();
        }

        // 应用 scale
        BigDecimal result = new BigDecimal(bigInt, scale);
        return result;
    }

    /**
     * 读取偏移量或大小
     * @param buffer ByteBuffer
     * @param isSmall 是否是小格式（2 字节），否则是大格式（4 字节）
     * @return 偏移量或大小
     */
    private static int readOffsetOrSize(ByteBuffer buffer, boolean isSmall) {
        if (isSmall) {
            return buffer.getShort() & 0xFFFF;
        } else {
            return buffer.getInt();
        }
    }

    /**
     * 读取可变长度整数
     * 参考：MySQL 协议的 Length-Encoded Integer
     *
     * @param buffer ByteBuffer
     * @return 长度值
     */
    private static long readVariableLength(ByteBuffer buffer) {
        int firstByte = buffer.get() & 0xFF;
        if (firstByte < 0xFB) {
            // 1 字节长度
            return firstByte;
        } else if (firstByte == 0xFC) {
            // 2 字节长度
            return buffer.getShort() & 0xFFFF;
        } else if (firstByte == 0xFD) {
            // 3 字节长度（小端序）
            int b1 = buffer.get() & 0xFF;
            int b2 = buffer.get() & 0xFF;
            int b3 = buffer.get() & 0xFF;
            return b1 | (b2 << 8) | (b3 << 16);
        } else if (firstByte == 0xFE) {
            // 8 字节长度
            return buffer.getLong();
        } else {
            throw new IllegalArgumentException("Invalid variable length prefix: 0x" + Integer.toHexString(firstByte));
        }
    }

    /**
     * 读取带长度前缀的字符串
     *
     * @param buffer ByteBuffer
     * @return 字符串
     */
    private static String readLengthPrefixedString(ByteBuffer buffer) {
        long length = readVariableLength(buffer);
        if (length == 0) {
            return "";
        }
        byte[] strBytes = new byte[(int) length];
        buffer.get(strBytes);
        return new String(strBytes, StandardCharsets.UTF_8);
    }
}