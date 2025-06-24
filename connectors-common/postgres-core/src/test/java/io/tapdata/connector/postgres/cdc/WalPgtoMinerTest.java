package io.tapdata.connector.postgres.cdc;

import org.junit.jupiter.api.Test;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WalPgtoMiner的测试类
 * 主要测试extractJsonObjects方法对JSON内部大括号的处理
 */
public class WalPgtoMinerTest {

    @Test
    public void testExtractJsonObjectsWithInnerBraces() throws Exception {
        WalPgtoMiner miner = new WalPgtoMiner();
        
        // 使用反射调用私有方法
        Method method = WalPgtoMiner.class.getDeclaredMethod("extractJsonObjects", String.class);
        method.setAccessible(true);
        
        // 测试用例1：JSON字符串内包含大括号
        String input1 = "{\"message\": \"Hello {world}\", \"data\": {\"nested\": \"value\"}}";
        @SuppressWarnings("unchecked")
        List<String> result1 = (List<String>) method.invoke(miner, input1);
        
        assertEquals(1, result1.size());
        assertEquals(input1, result1.get(0));
        
        // 测试用例2：多个JSON对象，其中包含内部大括号
        String input2 = "{\"msg\": \"User {admin} logged in\", \"id\": 1}{\"msg\": \"Data: {key: value}\", \"id\": 2}";
        @SuppressWarnings("unchecked")
        List<String> result2 = (List<String>) method.invoke(miner, input2);
        
        assertEquals(2, result2.size());
        assertEquals("{\"msg\": \"User {admin} logged in\", \"id\": 1}", result2.get(0));
        assertEquals("{\"msg\": \"Data: {key: value}\", \"id\": 2}", result2.get(1));
        
        // 测试用例3：包含转义引号的JSON
        String input3 = "{\"message\": \"He said \\\"Hello {world}\\\"\", \"status\": \"ok\"}";
        @SuppressWarnings("unchecked")
        List<String> result3 = (List<String>) method.invoke(miner, input3);
        
        assertEquals(1, result3.size());
        assertEquals(input3, result3.get(0));
        
        // 测试用例4：嵌套JSON对象
        String input4 = "{\"outer\": {\"inner\": {\"deep\": \"value with {braces}\"}}, \"simple\": \"text\"}";
        @SuppressWarnings("unchecked")
        List<String> result4 = (List<String>) method.invoke(miner, input4);
        
        assertEquals(1, result4.size());
        assertEquals(input4, result4.get(0));
        
        // 测试用例5：空字符串和无效输入
        String input5 = "";
        @SuppressWarnings("unchecked")
        List<String> result5 = (List<String>) method.invoke(miner, input5);
        assertEquals(0, result5.size());
        
        // 测试用例6：非JSON文本中的大括号
        String input6 = "Some text with {braces} but no JSON{\"valid\": \"json\"}more text";
        @SuppressWarnings("unchecked")
        List<String> result6 = (List<String>) method.invoke(miner, input6);
        
        assertEquals(1, result6.size());
        assertEquals("{\"valid\": \"json\"}", result6.get(0));
    }
    
    @Test
    public void testComplexJsonScenarios() throws Exception {
        WalPgtoMiner miner = new WalPgtoMiner();
        Method method = WalPgtoMiner.class.getDeclaredMethod("extractJsonObjects", String.class);
        method.setAccessible(true);
        
        // 测试用例：PostgreSQL WAL日志样式的JSON
        String walLogInput = "LOG: {\"xid\": \"12345\", \"lsn\": \"0/1234ABCD\", \"sql\": \"INSERT INTO users (name, data) VALUES ('John', '{\\\"preferences\\\": {\\\"theme\\\": \\\"dark\\\"}}')\", \"schemaname\": \"public\", \"relname\": \"users\"}";
        
        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(miner, walLogInput);
        
        assertEquals(1, result.size());
        String extractedJson = result.get(0);
        
        // 验证提取的JSON包含所有必要字段
        assertTrue(extractedJson.contains("\"xid\": \"12345\""));
        assertTrue(extractedJson.contains("\"lsn\": \"0/1234ABCD\""));
        assertTrue(extractedJson.contains("INSERT INTO users"));
        assertTrue(extractedJson.contains("{\\\"preferences\\\": {\\\"theme\\\": \\\"dark\\\"}}"));
        
        // 测试用例：多个WAL记录
        String multipleWalLogs = "{\"xid\": \"1\", \"sql\": \"UPDATE table SET data = '{\\\"key\\\": \\\"value\\\"}' WHERE id = 1\"}{\"xid\": \"2\", \"sql\": \"DELETE FROM table WHERE data LIKE '%{pattern}%'\"}";
        
        @SuppressWarnings("unchecked")
        List<String> multipleResult = (List<String>) method.invoke(miner, multipleWalLogs);
        
        assertEquals(2, multipleResult.size());
        assertTrue(multipleResult.get(0).contains("UPDATE table"));
        assertTrue(multipleResult.get(1).contains("DELETE FROM table"));
    }
    
    @Test
    public void testEdgeCases() throws Exception {
        WalPgtoMiner miner = new WalPgtoMiner();
        Method method = WalPgtoMiner.class.getDeclaredMethod("extractJsonObjects", String.class);
        method.setAccessible(true);
        
        // 测试用例：不完整的JSON
        String incompleteJson = "{\"incomplete\": \"json";
        @SuppressWarnings("unchecked")
        List<String> result1 = (List<String>) method.invoke(miner, incompleteJson);
        assertEquals(0, result1.size()); // 不完整的JSON应该被忽略
        
        // 测试用例：只有大括号
        String onlyBraces = "{}";
        @SuppressWarnings("unchecked")
        List<String> result2 = (List<String>) method.invoke(miner, onlyBraces);
        assertEquals(1, result2.size());
        assertEquals("{}", result2.get(0));
        
        // 测试用例：字符串中的转义反斜杠
        String escapedBackslash = "{\"path\": \"C:\\\\Users\\\\{username}\\\\file.txt\"}";
        @SuppressWarnings("unchecked")
        List<String> result3 = (List<String>) method.invoke(miner, escapedBackslash);
        assertEquals(1, result3.size());
        assertEquals(escapedBackslash, result3.get(0));
    }
}
