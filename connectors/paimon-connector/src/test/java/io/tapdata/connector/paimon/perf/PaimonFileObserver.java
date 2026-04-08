package io.tapdata.connector.paimon.perf;

import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Paimon 文件观测器 - 增强版（支持本地和 S3）
 * 自动扫描并输出Paimon数据目录的文件列表、文件大小、文件数量
 */
public class PaimonFileObserver {
    private final String warehousePath;
    private final String database;
    private final String tableName;
    private final boolean isS3;
    private final UnifiedFileSystem fileSystem;
    private List<FileInfo> lastScanFiles = new ArrayList<>();

    /**
     * 创建本地文件观测器
     */
    public PaimonFileObserver(String warehousePath, String database, String tableName) {
        this.warehousePath = warehousePath;
        this.database = database;
        this.tableName = tableName;
        this.isS3 = false;
        this.fileSystem = UnifiedFileSystem.createLocal();
    }

    /**
     * 创建 S3 文件观测器
     */
    public PaimonFileObserver(String warehousePath, String database, String tableName,
                              String s3Endpoint, String s3AccessKey, String s3SecretKey, String s3Region) {
        this.warehousePath = warehousePath;
        this.database = database;
        this.tableName = tableName;
        this.isS3 = true;
        // 从仓库路径中提取 bucket 名称 (格式: s3://bucket/key)
        String bucket = "default-bucket";
        if (warehousePath.startsWith("s3://")) {
            String pathWithoutScheme = warehousePath.substring(5);
            int slashIdx = pathWithoutScheme.indexOf('/');
            if (slashIdx > 0) {
                bucket = pathWithoutScheme.substring(0, slashIdx);
            } else {
                bucket = pathWithoutScheme;
            }
        }
        this.fileSystem = UnifiedFileSystem.createS3(s3Endpoint, s3AccessKey, s3SecretKey, s3Region, bucket);
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
        if (isS3) {
            // S3 路径格式：s3a://bucket/prefix/TC-01/default.db/test_table
            return warehousePath + "/" + database + ".db" + "/" + tableName;
        } else {
            return warehousePath + java.io.File.separator + database + ".db" + java.io.File.separator + tableName;
        }
    }

    /**
     * 扫描表的数据文件（支持 bucket-N/ 和 data/ 两种目录结构）
     */
    public List<FileInfo> scanDataFiles() throws IOException {
        List<FileInfo> fileInfos = new ArrayList<>();
        String tablePath = getTablePath();

        try {
            if (!fileSystem.exists(tablePath) || !fileSystem.isDirectory(tablePath)) {
                lastScanFiles = fileInfos;
                return fileInfos;
            }

            // 递归列出所有文件
            List<FileStatus> fileStatuses = fileSystem.listFilesRecursive(tablePath);
            
            for (FileStatus status : fileStatuses) {
                String fileName = status.getPath().getName();
                String pathStr = status.getPath().toString();
                
                // 只统计数据文件（parquet/orc/avro），排除元数据
                if ((fileName.endsWith(".parquet") ||
                     fileName.endsWith(".orc") ||
                     fileName.endsWith(".avro"))
                    && !pathStr.contains("/snapshot/")
                    && !pathStr.contains("/schema/")
                    && !pathStr.contains("/index/")
                    && !pathStr.contains("/manifest/")) {
                    fileInfos.add(new FileInfo(
                        status.getPath().toString(),
                        status.getLen(),
                        status.getModificationTime()
                    ));
                }
            }
        } catch (Exception e) {
            System.err.println("  [WARN] 扫描文件失败: " + e.getMessage());
            PerformanceTestRunner.printStackTrace(e);
        }

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
        if (isS3) {
            System.out.println("  存储类型: S3 对象存储");
        } else {
            System.out.println("  存储类型: 本地文件系统");
        }
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
        System.out.printf("  %-70s %12s%n", "文件路径", "大小");
        System.out.println("  " + "-".repeat(70) + " " + "-".repeat(12));

        int count = 0;
        String tablePath = getTableDataPath();
        for (FileInfo fileInfo : fileInfos) {
            if (count >= 20) break;
            // 显示相对路径
            String relativePath = fileInfo.getPath();
            if (relativePath.startsWith(tablePath)) {
                relativePath = relativePath.substring(tablePath.length() + 1);
            }
            System.out.printf("  %-70s %12s%n", relativePath, formatSize(fileInfo.getSize()));
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
     * 紧凑格式打印（至15个文件）
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
        String tablePath = getTablePath();
        for (int i = 0; i < shown; i++) {
            FileInfo fi = files.get(i);
            String rel = fi.getPath();
            if (rel.startsWith(tablePath)) {
                rel = rel.substring(tablePath.length() + 1);
            }
            System.out.printf("    %-65s %s%n", rel, formatSize(fi.getSize()));
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
