package io.tapdata.connector.starrocks.streamload;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * StarRocks多表缓存功能单元测试
 */
public class StarrocksStreamLoaderMultiTableTest {

    @Mock
    private StarrocksJdbcContext mockContext;

    @Mock
    private StarrocksConfig mockConfig;

    private CloseableHttpClient httpClient;
    private StarrocksStreamLoader streamLoader;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        
        // 设置mock配置
        when(mockContext.getConfig()).thenReturn(mockConfig);
        when(mockConfig.getFlushSizeMB()).thenReturn(100);
        when(mockConfig.getFlushTimeoutSeconds()).thenReturn(300);
        when(mockConfig.getMinuteLimitMB()).thenReturn(1000);
        when(mockConfig.getWriteByteBufferCapacity()).thenReturn(1024);
        when(mockConfig.getWriteFormatEnum()).thenReturn(StarrocksConfig.WriteFormat.json);
        
        httpClient = HttpClients.createDefault();
        streamLoader = new StarrocksStreamLoader(mockContext, httpClient);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (streamLoader != null) {
            streamLoader.shutdown();
        }
        if (httpClient != null) {
            httpClient.close();
        }
        
        // 清理测试产生的缓存文件
        cleanupTestCacheFiles();
    }

    @Test
    public void testMultiTableCacheFileSeparation() throws Exception {
        // 创建两个不同的表
        TapTable table1 = createTestTable("test_table_1", Arrays.asList("id", "name"));
        TapTable table2 = createTestTable("test_table_2", Arrays.asList("id", "email"));

        // 通过反射访问私有字段来验证缓存文件分离
        Field tempCacheFilesByTableField = StarrocksStreamLoader.class.getDeclaredField("tempCacheFilesByTable");
        tempCacheFilesByTableField.setAccessible(true);
        Map<String, Path> tempCacheFilesByTable = (Map<String, Path>) tempCacheFilesByTableField.get(streamLoader);

        Field cacheFileStreamsByTableField = StarrocksStreamLoader.class.getDeclaredField("cacheFileStreamsByTable");
        cacheFileStreamsByTableField.setAccessible(true);
        Map<String, ?> cacheFileStreamsByTable = (Map<String, ?>) cacheFileStreamsByTableField.get(streamLoader);

        // 初始状态应该为空
        assertTrue("初始状态缓存文件Map应该为空", tempCacheFilesByTable.isEmpty());
        assertTrue("初始状态文件流Map应该为空", cacheFileStreamsByTable.isEmpty());

        // 模拟初始化缓存文件
        java.lang.reflect.Method initMethod = StarrocksStreamLoader.class.getDeclaredMethod("initializeCacheFileForTable", String.class);
        initMethod.setAccessible(true);
        
        initMethod.invoke(streamLoader, "test_table_1");
        initMethod.invoke(streamLoader, "test_table_2");

        // 验证每个表都有独立的缓存文件
        assertEquals("应该有两个表的缓存文件", 2, tempCacheFilesByTable.size());
        assertEquals("应该有两个表的文件流", 2, cacheFileStreamsByTable.size());

        assertTrue("table1应该有缓存文件", tempCacheFilesByTable.containsKey("test_table_1"));
        assertTrue("table2应该有缓存文件", tempCacheFilesByTable.containsKey("test_table_2"));

        // 验证文件名包含表名
        Path table1File = tempCacheFilesByTable.get("test_table_1");
        Path table2File = tempCacheFilesByTable.get("test_table_2");

        assertNotNull("table1缓存文件不应该为null", table1File);
        assertNotNull("table2缓存文件不应该为null", table2File);

        assertTrue("table1文件名应该包含表名", table1File.getFileName().toString().contains("test_table_1"));
        assertTrue("table2文件名应该包含表名", table2File.getFileName().toString().contains("test_table_2"));

        // 验证文件名包含UUID（文件名格式：starrocks-cache-{tableName}-{uuid}-{threadId}-{timestamp}.tmp）
        String table1FileName = table1File.getFileName().toString();
        String table2FileName = table2File.getFileName().toString();

        String[] table1Parts = table1FileName.split("-");
        String[] table2Parts = table2FileName.split("-");

        assertTrue("table1文件名格式应该正确", table1Parts.length >= 6);
        assertTrue("table2文件名格式应该正确", table2Parts.length >= 6);

        assertEquals("table1文件名应该以starrocks开头", "starrocks", table1Parts[0]);
        assertEquals("table2文件名应该以starrocks开头", "starrocks", table2Parts[0]);
    }

    @Test
    public void testDataColumnsIndependentStorage() throws Exception {
        // 通过反射访问dataColumnsByTable字段
        Field dataColumnsByTableField = StarrocksStreamLoader.class.getDeclaredField("dataColumnsByTable");
        dataColumnsByTableField.setAccessible(true);
        Map<String, Set<String>> dataColumnsByTable = (Map<String, Set<String>>) dataColumnsByTableField.get(streamLoader);

        // 初始状态应该为空
        assertTrue("初始状态dataColumns应该为空", dataColumnsByTable.isEmpty());

        // 模拟为不同表设置不同的dataColumns
        Set<String> table1Columns = new HashSet<>(Arrays.asList("id", "name"));
        Set<String> table2Columns = new HashSet<>(Arrays.asList("id", "email"));

        dataColumnsByTable.put("test_table_1", table1Columns);
        dataColumnsByTable.put("test_table_2", table2Columns);

        // 验证每个表的dataColumns独立存储
        assertEquals("应该有两个表的dataColumns", 2, dataColumnsByTable.size());

        Set<String> retrievedTable1Columns = dataColumnsByTable.get("test_table_1");
        Set<String> retrievedTable2Columns = dataColumnsByTable.get("test_table_2");

        assertNotNull("table1的dataColumns不应该为null", retrievedTable1Columns);
        assertNotNull("table2的dataColumns不应该为null", retrievedTable2Columns);

        assertEquals("table1的dataColumns应该正确", table1Columns, retrievedTable1Columns);
        assertEquals("table2的dataColumns应该正确", table2Columns, retrievedTable2Columns);

        // 验证修改一个表的dataColumns不会影响另一个表
        table1Columns.add("age");
        dataColumnsByTable.put("test_table_1", table1Columns);

        Set<String> updatedTable1Columns = dataColumnsByTable.get("test_table_1");
        Set<String> unchangedTable2Columns = dataColumnsByTable.get("test_table_2");

        assertTrue("table1应该包含新增的列", updatedTable1Columns.contains("age"));
        assertFalse("table2不应该包含table1新增的列", unchangedTable2Columns.contains("age"));
    }

    @Test
    public void testPendingFlushTablesManagement() throws Exception {
        // 通过反射访问pendingFlushTables字段
        Field pendingFlushTablesField = StarrocksStreamLoader.class.getDeclaredField("pendingFlushTables");
        pendingFlushTablesField.setAccessible(true);
        Set<String> pendingFlushTables = (Set<String>) pendingFlushTablesField.get(streamLoader);

        // 初始状态应该为空
        assertTrue("初始状态待刷新表列表应该为空", pendingFlushTables.isEmpty());

        // 添加待刷新的表
        pendingFlushTables.add("test_table_1");
        pendingFlushTables.add("test_table_2");

        assertEquals("应该有两个待刷新的表", 2, pendingFlushTables.size());
        assertTrue("应该包含table1", pendingFlushTables.contains("test_table_1"));
        assertTrue("应该包含table2", pendingFlushTables.contains("test_table_2"));

        // 移除一个表
        pendingFlushTables.remove("test_table_1");

        assertEquals("应该只剩一个待刷新的表", 1, pendingFlushTables.size());
        assertFalse("不应该包含table1", pendingFlushTables.contains("test_table_1"));
        assertTrue("应该仍然包含table2", pendingFlushTables.contains("test_table_2"));
    }

    private TapTable createTestTable(String tableName, List<String> columnNames) {
        TapTable table = new TapTable(tableName);
        
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            TapField field = new TapField(columnName, "varchar");
            field.setPos(i + 1);
            table.add(field);
        }
        
        return table;
    }

    private void cleanupTestCacheFiles() {
        try {
            Path cacheDir = Paths.get(System.getProperty("java.io.tmpdir"), "starrocks-cache");
            if (Files.exists(cacheDir)) {
                File[] files = cacheDir.toFile().listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.getName().contains("test_table")) {
                            file.delete();
                        }
                    }
                }
            }
        } catch (Exception e) {
            // 忽略清理错误
        }
    }
}
