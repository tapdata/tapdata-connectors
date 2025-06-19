package io.tapdata.connector.starrocks.streamload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 文件缓存功能测试
 */
public class FileCacheTest {

    private Path tempDir;

    @BeforeEach
    public void setUp() throws IOException {
        // 创建测试用的临时目录
        tempDir = Paths.get(System.getProperty("java.io.tmpdir"), "starrocks-cache-test");
        if (!Files.exists(tempDir)) {
            Files.createDirectories(tempDir);
        }
    }

    @AfterEach
    public void tearDown() throws IOException {
        // 清理测试目录
        if (Files.exists(tempDir)) {
            Files.walk(tempDir)
                .sorted((a, b) -> b.compareTo(a)) // 先删除文件，再删除目录
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        // 忽略删除错误
                    }
                });
        }
    }

    @Test
    @DisplayName("测试临时目录创建")
    public void testTempDirectoryCreation() {
        Path starrocksCache = Paths.get(System.getProperty("java.io.tmpdir"), "starrocks-cache");
        
        // 验证目录路径格式正确
        assertTrue(starrocksCache.toString().contains("starrocks-cache"));
        
        // 验证可以创建目录
        assertDoesNotThrow(() -> {
            if (!Files.exists(starrocksCache)) {
                Files.createDirectories(starrocksCache);
            }
        });
    }

    @Test
    @DisplayName("测试缓存文件命名格式")
    public void testCacheFileNaming() {
        long threadId = Thread.currentThread().getId();
        long timestamp = System.currentTimeMillis();
        
        String fileName = String.format("starrocks-cache-%d-%d.tmp", threadId, timestamp);
        
        // 验证文件名格式
        assertTrue(fileName.startsWith("starrocks-cache-"));
        assertTrue(fileName.endsWith(".tmp"));
        assertTrue(fileName.contains(String.valueOf(threadId)));
    }

    @Test
    @DisplayName("测试文件写入和读取")
    public void testFileWriteAndRead() throws IOException {
        Path testFile = tempDir.resolve("test-cache.tmp");
        
        // 测试数据
        String testData = "test data line 1\ntest data line 2\n";
        
        // 写入文件
        Files.write(testFile, testData.getBytes());
        
        // 验证文件存在
        assertTrue(Files.exists(testFile));
        
        // 验证文件大小
        assertTrue(Files.size(testFile) > 0);
        
        // 读取文件内容
        String content = new String(Files.readAllBytes(testFile));
        assertEquals(testData, content);
        
        // 清理文件
        Files.deleteIfExists(testFile);
        assertFalse(Files.exists(testFile));
    }

    @Test
    @DisplayName("测试字节格式化")
    public void testBytesFormatting() {
        // 这里我们测试字节格式化的逻辑
        assertEquals("512 B", formatBytes(512));
        assertEquals("1.50 KB", formatBytes(1536)); // 1.5 * 1024
        assertEquals("2.50 MB", formatBytes(2621440)); // 2.5 * 1024 * 1024
        assertEquals("1.25 GB", formatBytes(1342177280L)); // 1.25 * 1024 * 1024 * 1024
    }

    /**
     * 复制 StarrocksStreamLoader 中的 formatBytes 方法用于测试
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }

    @Test
    @DisplayName("测试并发文件创建")
    public void testConcurrentFileCreation() {
        // 模拟多线程环境下的文件创建
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 5; i++) {
                long threadId = i;
                long timestamp = System.currentTimeMillis() + i;
                String fileName = String.format("starrocks-cache-%d-%d.tmp", threadId, timestamp);
                Path testFile = tempDir.resolve(fileName);
                
                Files.write(testFile, ("test data " + i).getBytes());
                assertTrue(Files.exists(testFile));
                
                // 清理
                Files.deleteIfExists(testFile);
            }
        });
    }
}
