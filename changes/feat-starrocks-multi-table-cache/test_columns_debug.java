import io.tapdata.connector.starrocks.streamload.StarrocksStreamLoader;
import io.tapdata.connector.starrocks.StarrocksJdbcContext;
import io.tapdata.connector.starrocks.bean.StarrocksConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.TapField;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.util.*;
import java.lang.reflect.Method;

/**
 * 测试StarRocks多表缓存功能的columns构建过程
 * 这个测试会打印详细的调试信息，显示：
 * 1. tableDataColumns的内容
 * 2. tapTable.getNameFieldMap()的内容
 * 3. columns构建过程
 * 4. 最终的columns结果
 */
public class TestColumnsDebug {
    
    public static void main(String[] args) {
        try {
            testColumnsBuilding();
            System.out.println("测试完成！请查看日志输出了解columns构建过程。");
        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static void testColumnsBuilding() throws Exception {
        // 创建测试配置
        StarrocksConfig config = createTestConfig();
        StarrocksJdbcContext context = new StarrocksJdbcContext(config);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        
        // 创建StreamLoader实例
        StarrocksStreamLoader loader = new StarrocksStreamLoader(context, httpClient);
        
        try {
            // 测试场景1: 单表数据写入
            testSingleTableColumns(loader);
            
            // 测试场景2: 多表数据写入
            testMultiTableColumns(loader);
            
            // 测试场景3: 不同列的表
            testDifferentColumnsTable(loader);
            
        } finally {
            // 清理
            loader.shutdown();
            httpClient.close();
        }
    }
    
    private static StarrocksConfig createTestConfig() {
        StarrocksConfig config = new StarrocksConfig();
        config.setStarrocksHttp("localhost:8030");
        config.setDatabase("test_db");
        config.setUser("root");
        config.setPassword("");
        config.setFlushSizeMB(100);
        config.setFlushTimeoutSeconds(300);
        config.setUniqueKeyType("Primary"); // 设置为Primary类型，不是Aggregate
        return config;
    }
    
    private static void testSingleTableColumns(StarrocksStreamLoader loader) throws Exception {
        System.out.println("\n=== 测试场景1: 单表数据写入 ===");
        
        // 创建测试表
        TapTable table1 = createTestTable("user_table", Arrays.asList("id", "name", "email", "age"));
        
        // 创建测试事件
        List<TapInsertRecordEvent> events = createTestEvents(table1, 2);
        
        // 模拟写入数据（这会触发startLoad和相关的日志输出）
        try {
            // 通过反射调用startLoad方法来设置dataColumns
            Method startLoadMethod = StarrocksStreamLoader.class.getDeclaredMethod("startLoad", 
                io.tapdata.entity.event.dml.TapRecordEvent.class, String.class);
            startLoadMethod.setAccessible(true);
            startLoadMethod.invoke(loader, events.get(0), table1.getId());
            
            // 模拟调用putFromFile方法来查看columns构建过程
            Method putFromFileMethod = StarrocksStreamLoader.class.getDeclaredMethod("putFromFile", TapTable.class);
            putFromFileMethod.setAccessible(true);
            
            System.out.println("调用putFromFile方法，查看columns构建过程...");
            // 注意：这个调用会失败，因为没有实际的StarRocks服务器，但会打印我们需要的调试信息
            try {
                putFromFileMethod.invoke(loader, table1);
            } catch (Exception e) {
                System.out.println("预期的异常（没有StarRocks服务器）: " + e.getCause().getClass().getSimpleName());
            }
            
        } catch (Exception e) {
            System.out.println("调用过程中的异常: " + e.getMessage());
        }
    }
    
    private static void testMultiTableColumns(StarrocksStreamLoader loader) throws Exception {
        System.out.println("\n=== 测试场景2: 多表数据写入 ===");
        
        // 创建两个不同的表
        TapTable table1 = createTestTable("orders_table", Arrays.asList("order_id", "customer_id", "amount"));
        TapTable table2 = createTestTable("products_table", Arrays.asList("product_id", "name", "price", "category"));
        
        // 为每个表创建测试事件
        List<TapInsertRecordEvent> events1 = createTestEvents(table1, 1);
        List<TapInsertRecordEvent> events2 = createTestEvents(table2, 1);
        
        try {
            // 通过反射调用startLoad方法
            Method startLoadMethod = StarrocksStreamLoader.class.getDeclaredMethod("startLoad", 
                io.tapdata.entity.event.dml.TapRecordEvent.class, String.class);
            startLoadMethod.setAccessible(true);
            
            Method putFromFileMethod = StarrocksStreamLoader.class.getDeclaredMethod("putFromFile", TapTable.class);
            putFromFileMethod.setAccessible(true);
            
            // 处理第一个表
            System.out.println("处理第一个表: " + table1.getId());
            startLoadMethod.invoke(loader, events1.get(0), table1.getId());
            try {
                putFromFileMethod.invoke(loader, table1);
            } catch (Exception e) {
                System.out.println("预期的异常: " + e.getCause().getClass().getSimpleName());
            }
            
            // 处理第二个表
            System.out.println("处理第二个表: " + table2.getId());
            startLoadMethod.invoke(loader, events2.get(0), table2.getId());
            try {
                putFromFileMethod.invoke(loader, table2);
            } catch (Exception e) {
                System.out.println("预期的异常: " + e.getCause().getClass().getSimpleName());
            }
            
        } catch (Exception e) {
            System.out.println("调用过程中的异常: " + e.getMessage());
        }
    }
    
    private static void testDifferentColumnsTable(StarrocksStreamLoader loader) throws Exception {
        System.out.println("\n=== 测试场景3: 不同列数的表 ===");
        
        // 创建一个只有部分列有数据的表
        TapTable table = createTestTable("sparse_table", Arrays.asList("id", "name", "email", "phone", "address"));
        
        // 创建一个只包含部分列的事件
        TapInsertRecordEvent event = new TapInsertRecordEvent();
        Map<String, Object> after = new HashMap<>();
        after.put("id", "1");
        after.put("name", "Test User");
        // 注意：这里故意不包含email, phone, address列
        
        event.setAfter(after);
        event.setTableId(table.getId());
        
        try {
            Method startLoadMethod = StarrocksStreamLoader.class.getDeclaredMethod("startLoad", 
                io.tapdata.entity.event.dml.TapRecordEvent.class, String.class);
            startLoadMethod.setAccessible(true);
            
            Method putFromFileMethod = StarrocksStreamLoader.class.getDeclaredMethod("putFromFile", TapTable.class);
            putFromFileMethod.setAccessible(true);
            
            System.out.println("处理稀疏数据表: " + table.getId());
            startLoadMethod.invoke(loader, event, table.getId());
            try {
                putFromFileMethod.invoke(loader, table);
            } catch (Exception e) {
                System.out.println("预期的异常: " + e.getCause().getClass().getSimpleName());
            }
            
        } catch (Exception e) {
            System.out.println("调用过程中的异常: " + e.getMessage());
        }
    }
    
    private static TapTable createTestTable(String tableName, List<String> columnNames) {
        TapTable table = new TapTable(tableName);
        
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            TapField field = new TapField(columnName, "varchar");
            field.setPos(i + 1);
            table.add(field);
        }
        
        return table;
    }
    
    private static List<TapInsertRecordEvent> createTestEvents(TapTable table, int count) {
        List<TapInsertRecordEvent> events = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            TapInsertRecordEvent event = new TapInsertRecordEvent();
            Map<String, Object> after = new HashMap<>();
            
            // 为每个字段生成测试数据
            for (String fieldName : table.getNameFieldMap().keySet()) {
                after.put(fieldName, "test_value_" + fieldName + "_" + i);
            }
            
            event.setAfter(after);
            event.setTableId(table.getId());
            events.add(event);
        }
        
        return events;
    }
}
