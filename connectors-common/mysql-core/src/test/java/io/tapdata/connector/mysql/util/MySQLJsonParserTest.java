package io.tapdata.connector.mysql.util;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MySQL JSON 二进制格式解析器测试
 * 
 * @author TapData
 */
class MySQLJsonParserTest {

    @Test
    void testParseNull() {
        assertNull(MySQLJsonParser.parseMySQLJsonBinary(null));
        assertNull(MySQLJsonParser.parseMySQLJsonBinary(new byte[0]));
    }

    @Test
    void testParseMariaDBJsonString() {
        // MariaDB 格式的 JSON 字符串（第一个字节 > 0x0F）
        String jsonStr = "{\"name\":\"test\"}";
        byte[] data = jsonStr.getBytes();
        String result = MySQLJsonParser.parseMySQLJsonBinary(data);
        assertEquals(jsonStr, result);
    }

    @Test
    void testParseLiteralNull() {
        // Type: LITERAL (0x04), Literal Type: NULL (0x00)
        byte[] data = {0x04, 0x00};
        String result = MySQLJsonParser.parseMySQLJsonBinary(data);
        assertEquals("null", result);
    }

    @Test
    void testParseLiteralTrue() {
        // Type: LITERAL (0x04), Literal Type: TRUE (0x01)
        byte[] data = {0x04, 0x01};
        String result = MySQLJsonParser.parseMySQLJsonBinary(data);
        assertEquals("true", result);
    }

    @Test
    void testParseLiteralFalse() {
        // Type: LITERAL (0x04), Literal Type: FALSE (0x02)
        byte[] data = {0x04, 0x02};
        String result = MySQLJsonParser.parseMySQLJsonBinary(data);
        assertEquals("false", result);
    }

    @Test
    void testParseInt16() {
        // Type: INT16 (0x05), Value: 100
        ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x05);
        buffer.putShort((short) 100);
        
        String result = MySQLJsonParser.parseMySQLJsonBinary(buffer.array());
        assertEquals("100", result);
    }

    @Test
    void testParseInt32() {
        // Type: INT32 (0x07), Value: 100000
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x07);
        buffer.putInt(100000);
        
        String result = MySQLJsonParser.parseMySQLJsonBinary(buffer.array());
        assertEquals("100000", result);
    }

    @Test
    void testParseInt64() {
        // Type: INT64 (0x09), Value: 9223372036854775807
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x09);
        buffer.putLong(9223372036854775807L);
        
        String result = MySQLJsonParser.parseMySQLJsonBinary(buffer.array());
        assertEquals("9223372036854775807", result);
    }

    @Test
    void testParseDouble() {
        // Type: DOUBLE (0x0B), Value: 3.14159
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x0B);
        buffer.putDouble(3.14159);
        
        String result = MySQLJsonParser.parseMySQLJsonBinary(buffer.array());
        assertTrue(result.startsWith("3.14159"));
    }

    @Test
    void testParseString() {
        // Type: STRING (0x0C), Length: 5, Value: "hello"
        String str = "hello";
        ByteBuffer buffer = ByteBuffer.allocate(7);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x0C);
        buffer.put((byte) str.length()); // variable length: 5
        buffer.put(str.getBytes());
        
        String result = MySQLJsonParser.parseMySQLJsonBinary(buffer.array());
        assertEquals("\"hello\"", result);
    }

    @Test
    void testParseEmptyObject() {
        // Type: SMALL_OBJECT (0x00), Element Count: 0, Bytes: 0
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x00);
        buffer.putShort((short) 0); // element count
        buffer.putShort((short) 0); // bytes
        
        String result = MySQLJsonParser.parseMySQLJsonBinary(buffer.array());
        assertEquals("{}", result);
    }

    @Test
    void testParseEmptyArray() {
        // Type: SMALL_ARRAY (0x02), Element Count: 0, Bytes: 0
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x02);
        buffer.putShort((short) 0); // element count
        buffer.putShort((short) 0); // bytes
        
        String result = MySQLJsonParser.parseMySQLJsonBinary(buffer.array());
        assertEquals("[]", result);
    }

    @Test
    void testParseInvalidData() {
        // 无效的类型 - 0xFF 会被当作 MariaDB 格式的字符串
        byte[] data = {(byte) 0xFF};
        String result = MySQLJsonParser.parseMySQLJsonBinary(data);
        // MariaDB 格式会返回字符串，而不是 null
        assertNotNull(result);

        // 测试真正无效的数据（类型码在有效范围内但数据不足）
        byte[] invalidData = {0x05}; // INT16 类型但没有数据
        String invalidResult = MySQLJsonParser.parseMySQLJsonBinary(invalidData);
        assertNull(invalidResult);
    }
}

