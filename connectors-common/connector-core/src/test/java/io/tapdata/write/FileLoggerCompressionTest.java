package io.tapdata.write;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for FileLogger compression functionality
 */
public class FileLoggerCompressionTest {

    @TempDir
    Path tempDir;

    private FileLogger logger;

    @AfterEach
    public void tearDown() {
        if (logger != null) {
            logger.close();
        }
    }

    @Test
    public void testCompressionEnabled() throws IOException, InterruptedException {
        // Create logger with compression enabled (compress every 2 seconds for testing)
        logger = FileLogger.builder()
                .logDirectory(tempDir.toString())
                .logFilePrefix("test")
                .maxFileSizeMB(1)
                .enableCompression(true)
                .compressIntervalMs(2000)  // 2 seconds for testing
                .retainDays(1)
                .build();

        // Write some logs
        for (int i = 0; i < 100; i++) {
            logger.write("Test log message " + i);
        }

        // Force flush and close to ensure file is written
        logger.forceFlush();
        Thread.sleep(500);

        // Close the logger to release the file
        logger.close();

        // Get the log file path after closing
        List<Path> logFiles = Files.list(tempDir)
                .filter(p -> p.getFileName().toString().endsWith(".log"))
                .collect(Collectors.toList());

        assertTrue(logFiles.size() > 0, "Should have at least one log file");
        Path logFile = logFiles.get(0);
        long originalSize = Files.size(logFile);
        System.out.println("Original log file: " + logFile.getFileName() + ", size: " + originalSize);

        // Create a new logger to trigger compression
        logger = FileLogger.builder()
                .logDirectory(tempDir.toString())
                .logFilePrefix("test")
                .enableCompression(true)
                .compressIntervalMs(2000)
                .retainDays(1)
                .build();

        // Manually trigger compression (force all files)
        logger.compressNow(true);

        // Wait for compression to complete
        Thread.sleep(1000);

        // Check that compressed file exists
        List<Path> compressedFiles = Files.list(tempDir)
                .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                .collect(Collectors.toList());

        assertTrue(compressedFiles.size() > 0, "Should have at least one compressed file");

        // Check that original log file was deleted
        assertFalse(Files.exists(logFile), "Original log file should be deleted after compression");

        // Check compressed file size
        Path compressedFile = compressedFiles.get(0);
        long compressedSize = Files.size(compressedFile);

        System.out.println("Compressed file: " + compressedFile.getFileName() + ", size: " + compressedSize);
        System.out.println("Compression ratio: " + (100.0 - (double) compressedSize / originalSize * 100) + "%");

        // Compressed file should be smaller
        assertTrue(compressedSize < originalSize, "Compressed file should be smaller than original");

        // Check stats
        FileLogger.LoggerStats stats = logger.getStats();
        assertTrue(stats.getTotalFilesCompressed() > 0, "Should have compressed at least one file");

        System.out.println("Stats: " + stats);
    }

    @Test
    public void testCompressionDisabled() throws IOException, InterruptedException {
        // Create logger without compression
        logger = FileLogger.builder()
                .logDirectory(tempDir.toString())
                .logFilePrefix("test-no-compress")
                .maxFileSizeMB(1)
                .enableCompression(false)  // Compression disabled
                .build();

        // Write some logs
        for (int i = 0; i < 100; i++) {
            logger.write("Test log message " + i);
        }

        logger.forceFlush();
        Thread.sleep(500);

        // Check that log file exists
        List<Path> logFiles = Files.list(tempDir)
                .filter(p -> p.getFileName().toString().startsWith("test-no-compress"))
                .filter(p -> p.getFileName().toString().endsWith(".log"))
                .collect(Collectors.toList());

        assertTrue(logFiles.size() > 0, "Should have at least one log file");

        // Wait a bit
        Thread.sleep(2000);

        // Check that no compressed files exist
        List<Path> compressedFiles = Files.list(tempDir)
                .filter(p -> p.getFileName().toString().startsWith("test-no-compress"))
                .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                .collect(Collectors.toList());

        assertEquals(0, compressedFiles.size(), "Should have no compressed files when compression is disabled");

        // Check stats
        FileLogger.LoggerStats stats = logger.getStats();
        assertEquals(0, stats.getTotalFilesCompressed(), "Should have compressed zero files");
    }

    @Test
    public void testManualCompression() throws IOException, InterruptedException {
        // Create logger with compression enabled
        logger = FileLogger.builder()
                .logDirectory(tempDir.toString())
                .logFilePrefix("test-manual")
                .maxFileSizeMB(1)
                .enableCompression(true)
                .compressIntervalMs(60000)  // 1 minute (won't trigger automatically in test)
                .build();

        // Write logs and rotate file
        for (int i = 0; i < 100; i++) {
            logger.write("Test log message " + i);
        }

        logger.forceFlush();
        Thread.sleep(500);

        // Close to release the file
        logger.close();

        // Create new logger
        logger = FileLogger.builder()
                .logDirectory(tempDir.toString())
                .logFilePrefix("test-manual")
                .enableCompression(true)
                .compressIntervalMs(60000)
                .build();

        // Manually trigger compression (force all files)
        logger.compressNow(true);
        Thread.sleep(1000);

        // Check that compressed file exists
        List<Path> compressedFiles = Files.list(tempDir)
                .filter(p -> p.getFileName().toString().startsWith("test-manual"))
                .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                .collect(Collectors.toList());

        assertTrue(compressedFiles.size() > 0, "Manual compression should create compressed files");

        System.out.println("Manual compression created " + compressedFiles.size() + " compressed file(s)");
    }
}

