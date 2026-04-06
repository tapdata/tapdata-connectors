package io.tapdata.connector.paimon.perf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Paimon 文件观测器 - 增强版
 * 自动扫描并输出Paimon数据目录的文件列表、文件大小、文件数量
 */
public class PaimonFileObserver {
    private final String warehousePath;
    private final String database;
    private final String tableName;
    private List<FileInfo> lastScanFiles = new ArrayList<>();

    public PaimonFileObserver(String warehousePath, String database, String tableName) {
        this.warehousePath = warehousePath;
        this.database = database;
        this.tableName = tableName;
    }

    /**
     * 获取表的数据目录路径（兼容 bucket-N 和 data/ 两种布局）
     */
    public String getTableDataPath() {
        // Paimon 1.x 动态分桶下数据直接在表根目录下的 bucket-N/ 中
        return getTablePath();
    }

    /**
     * 获取表的完整目录路径
     */
    public String getTablePath() {
        return warehousePath + File.separator + database + ".db" + File.separator + tableName;
    }

    /**
     * 扫描表的数据文件（支持 bucket-N/ 和 data/ 两种目录结构）
     */
    public List<FileInfo> scanDataFiles() throws IOException {
        List<FileInfo> fileInfos = new ArrayList<>();
        Path tablePath = Paths.get(getTablePath());

        if (!Files.exists(tablePath) || !Files.isDirectory(tablePath)) {
            lastScanFiles = fileInfos;
            return fileInfos;
        }

        Files.walk(tablePath)
                .filter(Files::isRegularFile)
                .filter(path -> {
                    String fileName = path.getFileName().toString();
                    // 只统计数据文件（parquet/orc/avro），排除元数据
                    return (fileName.endsWith(".parquet") ||
                            fileName.endsWith(".orc") ||
                            fileName.endsWith(".avro"))
                           && !path.toString().contains(File.separator + "snapshot" + File.separator)
                           && !path.toString().contains(File.separator + "schema" + File.separator)
                           && !path.toString().contains(File.separator + "index" + File.separator)
                           && !path.toString().contains(File.separator + "manifest" + File.separator);
                })
                .forEach(path -> {
                    try {
                        File file = path.toFile();
                        fileInfos.add(new FileInfo(file.getPath(), file.length(), file.lastModified()));
                    } catch (Exception ignored) {}
                });

        lastScanFiles = fileInfos;
        return fileInfos;
    }

    /**
     * 打印详细文件信息
     */
    public void printFileInfo() throws IOException {
        List<FileInfo> fileInfos = scanDataFiles();
        printFileInfo(fileInfos);
    }

    /**
     * 打印文件信息(使用已扫描的结果)
     */
    public void printFileInfo(List<FileInfo> fileInfos) throws IOException {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("  Paimon 数据文件观测报告");
        System.out.println("=".repeat(70));
        System.out.println("表路径: " + getTableDataPath());
        System.out.println("扫描时间: " + new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        System.out.println("-".repeat(70));

        if (fileInfos.isEmpty()) {
            System.out.println("⚠ 未找到数据文件!");
            System.out.println("=".repeat(70));
            return;
        }

        // 基础统计
        long totalSize = fileInfos.stream().mapToLong(FileInfo::getSize).sum();
        long minSize = fileInfos.stream().mapToLong(FileInfo::getSize).min().orElse(0);
        long maxSize = fileInfos.stream().mapToLong(FileInfo::getSize).max().orElse(0);
        double avgSize = (double) totalSize / fileInfos.size();

        System.out.println("📊 文件统计:");
        System.out.println("  文件总数: " + fileInfos.size() + " 个");
        System.out.println("  总大小: " + formatSize(totalSize));
        System.out.println("  平均大小: " + formatSize((long) avgSize));
        System.out.println("  最小文件: " + formatSize(minSize));
        System.out.println("  最大文件: " + formatSize(maxSize));

        // 文件大小分布
        System.out.println("\n📏 文件大小分布:");
        Map<String, Long> sizeDistribution = calculateSizeDistribution(fileInfos);
        for (Map.Entry<String, Long> entry : sizeDistribution.entrySet()) {
            String bar = "█".repeat(Math.max(1, entry.getValue().intValue()));
            System.out.printf("  %-20s: %3d 个 %s%n", entry.getKey(), entry.getValue(), bar);
        }

        // 文件列表(前20个)
        System.out.println("\n📄 文件列表(前20个):");
        System.out.printf("  %-60s %12s%n", "文件路径", "大小");
        System.out.println("  " + "-".repeat(60) + " " + "-".repeat(12));
        
        int count = 0;
        for (FileInfo fileInfo : fileInfos) {
            if (count >= 20) break;
            String relativePath = fileInfo.getPath().replace(getTableDataPath() + File.separator, "");
            System.out.printf("  %-60s %12s%n", relativePath, formatSize(fileInfo.getSize()));
            count++;
        }
        
        if (fileInfos.size() > 20) {
            System.out.println("  ... 还有 " + (fileInfos.size() - 20) + " 个文件");
        }

        System.out.println("=".repeat(70));
    }

    /**
     * 计算文件大小分布
     */
    private Map<String, Long> calculateSizeDistribution(List<FileInfo> fileInfos) {
        Map<String, Long> distribution = new LinkedHashMap<>();
        distribution.put("< 1KB", fileInfos.stream().filter(f -> f.getSize() < 1024).count());
        distribution.put("1KB - 1MB", fileInfos.stream().filter(f -> 
                f.getSize() >= 1024 && f.getSize() < 1024 * 1024).count());
        distribution.put("1MB - 10MB", fileInfos.stream().filter(f -> 
                f.getSize() >= 1024 * 1024 && f.getSize() < 10 * 1024 * 1024).count());
        distribution.put("10MB - 100MB", fileInfos.stream().filter(f -> 
                f.getSize() >= 10 * 1024 * 1024 && f.getSize() < 100 * 1024 * 1024).count());
        distribution.put("100MB - 500MB", fileInfos.stream().filter(f -> 
                f.getSize() >= 100 * 1024 * 1024 && f.getSize() < 500 * 1024 * 1024).count());
        distribution.put("> 500MB", fileInfos.stream().filter(f -> 
                f.getSize() >= 500 * 1024 * 1024).count());
        return distribution;
    }

    /**
     * 监控文件变化
     */
    public void monitorFileChanges(long durationMs) throws IOException, InterruptedException {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("  开始监控文件变化 (持续时间: " + (durationMs / 1000) + "秒)");
        System.out.println("=".repeat(70));
        
        List<FileInfo> initialFiles = scanDataFiles();
        System.out.println("初始文件数量: " + initialFiles.size());
        System.out.println("初始总大小: " + formatSize(initialFiles.stream().mapToLong(FileInfo::getSize).sum()));

        long startTime = System.currentTimeMillis();
        int checkCount = 0;
        
        while (System.currentTimeMillis() - startTime < durationMs) {
            TimeUnit.SECONDS.sleep(2);
            List<FileInfo> currentFiles = scanDataFiles();
            long currentSize = currentFiles.stream().mapToLong(FileInfo::getSize).sum();
            checkCount++;
            
            System.out.printf("  [%ds] 检查#%d: 文件数=%d, 总大小=%s, 新增文件=%d%n",
                    (System.currentTimeMillis() - startTime) / 1000,
                    checkCount,
                    currentFiles.size(),
                    formatSize(currentSize),
                    currentFiles.size() - initialFiles.size());
            
            initialFiles = currentFiles;
        }

        System.out.println("监控结束");
    }

    /**
     * 对比两次扫描的差异
     */
    public FileChangeReport compareWithLastScan() throws IOException {
        List<FileInfo> currentFiles = scanDataFiles();
        FileChangeReport report = new FileChangeReport();
        
        report.setPreviousFileCount(lastScanFiles.size());
        report.setCurrentFileCount(currentFiles.size());
        report.setNewFiles(currentFiles.size() - lastScanFiles.size());
        
        long previousSize = lastScanFiles.stream().mapToLong(FileInfo::getSize).sum();
        long currentSize = currentFiles.stream().mapToLong(FileInfo::getSize).sum();
        report.setPreviousSize(previousSize);
        report.setCurrentSize(currentSize);
        report.setSizeChange(currentSize - previousSize);
        
        return report;
    }

    /**
     * 格式化文件大小
     */
    public static String formatSize(long size) {
        if (size < 1024) {
            return size + " B";
        } else if (size < 1024 * 1024) {
            return String.format("%.2f KB", (double) size / 1024);
        } else if (size < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", (double) size / (1024 * 1024));
        } else {
            return String.format("%.2f GB", (double) size / (1024 * 1024 * 1024));
        }
    }

    /**
     * 文件信息类
     */
    public static class FileInfo {
        private final String path;
        private final long size;
        private final long lastModified;

        public FileInfo(String path, long size, long lastModified) {
            this.path = path;
            this.size = size;
            this.lastModified = lastModified;
        }

        public String getPath() { return path; }
        public long getSize() { return size; }
        public long getLastModified() { return lastModified; }

        @Override
        public String toString() {
            return String.format("FileInfo{path='%s', size=%s, lastModified=%d}",
                    path, formatSize(size), lastModified);
        }
    }

    /**
     * 文件变化报告
     */
    public static class FileChangeReport {
        private int previousFileCount;
        private int currentFileCount;
        private int newFiles;
        private long previousSize;
        private long currentSize;
        private long sizeChange;

        public int getPreviousFileCount() { return previousFileCount; }
        public void setPreviousFileCount(int previousFileCount) { this.previousFileCount = previousFileCount; }
        public int getCurrentFileCount() { return currentFileCount; }
        public void setCurrentFileCount(int currentFileCount) { this.currentFileCount = currentFileCount; }
        public int getNewFiles() { return newFiles; }
        public void setNewFiles(int newFiles) { this.newFiles = newFiles; }
        public long getPreviousSize() { return previousSize; }
        public void setPreviousSize(long previousSize) { this.previousSize = previousSize; }
        public long getCurrentSize() { return currentSize; }
        public void setCurrentSize(long currentSize) { this.currentSize = currentSize; }
        public long getSizeChange() { return sizeChange; }
        public void setSizeChange(long sizeChange) { this.sizeChange = sizeChange; }

        @Override
        public String toString() {
            return String.format("文件变化: 数量 %d -> %d (新增%d), 大小 %s -> %s (变化%s)",
                    previousFileCount, currentFileCount, newFiles,
                    formatSize(previousSize), formatSize(currentSize), 
                    (sizeChange >= 0 ? "+" : "") + formatSize(sizeChange));
        }
    }

    /**
     * 扫描所有文件（包含 data 目录和其他元数据目录下的数据文件）
     */
    public List<FileInfo> scanAllFiles() throws IOException {
        return scanDataFiles();
    }

    /**
     * 紧凑格式打印（最多15个文件）
     */
    public void printCompact() throws IOException {
        List<FileInfo> files = scanDataFiles();
        if (files.isEmpty()) {
            System.out.println("  [文件] 暂无数据文件（可能尚未 flush）");
            return;
        }
        long total = files.stream().mapToLong(FileInfo::getSize).sum();
        System.out.printf("  [文件] 数量: %d  总大小: %s%n", files.size(), formatSize(total));
        int shown = Math.min(files.size(), 15);
        for (int i = 0; i < shown; i++) {
            FileInfo fi = files.get(i);
            String rel = fi.getPath().replace(getTablePath() + File.separator, "");
            System.out.printf("    %-55s %s%n", rel, formatSize(fi.getSize()));
        }
        if (files.size() > 15) {
            System.out.printf("    ... 还有 %d 个文件%n", files.size() - 15);
        }
    }

    /**
     * 获取最后扫描的文件列表
     */
    public List<FileInfo> getLastScanFiles() {
        return lastScanFiles;
    }
}
