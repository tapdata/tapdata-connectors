package io.tapdata.connector.postgres.util;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 完全模拟 tar -xvf 命令的解压工具
 * 正确处理所有 tar 文件特性：权限、符号链接、硬链接、时间戳等
 */
public class TarExtractor {

    private static final boolean VERBOSE = true;
    private static final boolean PRESERVE_PERMISSIONS = true;
    private static final boolean PRESERVE_TIMESTAMPS = true;

    /**
     * 解压 tar.gz 文件，完全模拟 tar -xvf 行为
     */
    public static void extractTarGz(InputStream inputStream, String outputDir) throws IOException {
        Path outputPath = Paths.get(outputDir);
        if (!Files.exists(outputPath)) {
            Files.createDirectories(outputPath);
        }

        System.out.println("=== 开始解压 (模拟 tar -xvf) ===");
        System.out.println("目标目录: " + outputPath.toAbsolutePath());

        // 存储所有条目信息，用于后处理
        List<TarEntryInfo> allEntries = new ArrayList<>();
        Map<String, String> symbolicLinks = new LinkedHashMap<>();
        Map<String, String> hardLinks = new LinkedHashMap<>();

        try (BufferedInputStream bufferedInput = new BufferedInputStream(inputStream);
             GzipCompressorInputStream gzipInput = new GzipCompressorInputStream(bufferedInput);
             TarArchiveInputStream tarInput = new TarArchiveInputStream(gzipInput)) {

            TarArchiveEntry entry;
            while ((entry = tarInput.getNextTarEntry()) != null) {
                TarEntryInfo entryInfo = new TarEntryInfo(entry);
                allEntries.add(entryInfo);

                if (VERBOSE) {
                    System.out.println(formatTarEntry(entry));
                }

                // 根据条目类型处理
                if (entry.isDirectory()) {
                    extractDirectory(entry, outputPath);
                } else if (entry.isFile()) {
                    extractFile(tarInput, entry, outputPath);
                } else if (entry.isSymbolicLink()) {
                    // 收集符号链接，稍后处理
                    symbolicLinks.put(entry.getName(), entry.getLinkName());
                } else if (entry.isLink()) {
                    // 收集硬链接，稍后处理
                    hardLinks.put(entry.getName(), entry.getLinkName());
                } else {
                    System.out.println("⚠️  跳过未知类型: " + entry.getName());
                }
            }
        }

        // 后处理：创建符号链接和硬链接
        System.out.println("\n=== 后处理阶段 ===");
        createHardLinks(hardLinks, outputPath);
        createSymbolicLinks(symbolicLinks, outputPath);

        // 设置权限和时间戳
        if (PRESERVE_PERMISSIONS || PRESERVE_TIMESTAMPS) {
            System.out.println("\n=== 设置权限和时间戳 ===");
            setPermissionsAndTimestamps(allEntries, outputPath);
        }

        System.out.println("\n✅ 解压完成");
    }

    /**
     * 格式化 tar 条目信息（模拟 tar -v 输出）
     */
    private static String formatTarEntry(TarArchiveEntry entry) {
        StringBuilder sb = new StringBuilder();
        
        // 文件类型和权限
        sb.append(formatPermissions(entry));
        sb.append(" ");
        
        // 用户/组（简化）
        sb.append(String.format("%8s/%-8s", 
                entry.getUserName() != null ? entry.getUserName() : String.valueOf(entry.getLongUserId()),
                entry.getGroupName() != null ? entry.getGroupName() : String.valueOf(entry.getLongGroupId())));
        sb.append(" ");
        
        // 大小
        if (entry.isDirectory()) {
            sb.append(String.format("%10s", "0"));
        } else {
            sb.append(String.format("%10d", entry.getSize()));
        }
        sb.append(" ");
        
        // 时间戳
        Date modTime = entry.getModTime();
        if (modTime != null) {
            sb.append(String.format("%tF %<tT", modTime));
        } else {
            sb.append("                   ");
        }
        sb.append(" ");
        
        // 文件名
        sb.append(entry.getName());
        
        // 链接信息
        if (entry.isSymbolicLink()) {
            sb.append(" -> ").append(entry.getLinkName());
        } else if (entry.isLink()) {
            sb.append(" link to ").append(entry.getLinkName());
        }
        
        return sb.toString();
    }

    /**
     * 格式化权限信息（类似 ls -l）
     */
    private static String formatPermissions(TarArchiveEntry entry) {
        char[] perms = new char[10];
        
        // 文件类型
        if (entry.isDirectory()) {
            perms[0] = 'd';
        } else if (entry.isSymbolicLink()) {
            perms[0] = 'l';
        } else if (entry.isLink()) {
            perms[0] = '-'; // 硬链接显示为普通文件
        } else {
            perms[0] = '-';
        }
        
        // 权限位
        int mode = entry.getMode();
        perms[1] = (mode & 0400) != 0 ? 'r' : '-';
        perms[2] = (mode & 0200) != 0 ? 'w' : '-';
        perms[3] = (mode & 0100) != 0 ? 'x' : '-';
        perms[4] = (mode & 0040) != 0 ? 'r' : '-';
        perms[5] = (mode & 0020) != 0 ? 'w' : '-';
        perms[6] = (mode & 0010) != 0 ? 'x' : '-';
        perms[7] = (mode & 0004) != 0 ? 'r' : '-';
        perms[8] = (mode & 0002) != 0 ? 'w' : '-';
        perms[9] = (mode & 0001) != 0 ? 'x' : '-';
        
        return new String(perms);
    }

    /**
     * 提取目录
     */
    private static void extractDirectory(TarArchiveEntry entry, Path outputPath) throws IOException {
        Path dirPath = outputPath.resolve(entry.getName());
        
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }
    }

    /**
     * 提取文件
     */
    private static void extractFile(TarArchiveInputStream tarInput, TarArchiveEntry entry, Path outputPath) throws IOException {
        Path filePath = outputPath.resolve(entry.getName());
        
        // 确保父目录存在
        Path parentDir = filePath.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }

        // 提取文件内容
        long fileSize = entry.getSize();
        try (OutputStream output = Files.newOutputStream(filePath)) {
            byte[] buffer = new byte[8192];
            long totalRead = 0;
            
            while (totalRead < fileSize) {
                long remaining = fileSize - totalRead;
                int toRead = (int) Math.min(buffer.length, remaining);
                
                int bytesRead = tarInput.read(buffer, 0, toRead);
                if (bytesRead == -1) {
                    throw new IOException("Unexpected end of stream for file: " + entry.getName() + 
                                        ". Expected " + fileSize + " bytes, got " + totalRead);
                }
                
                output.write(buffer, 0, bytesRead);
                totalRead += bytesRead;
            }
        }

        // 验证文件大小
        long actualSize = Files.size(filePath);
        if (actualSize != fileSize) {
            throw new IOException("File size mismatch for " + entry.getName() + 
                                ". Expected: " + fileSize + ", Actual: " + actualSize);
        }
    }

    /**
     * 创建硬链接
     */
    private static void createHardLinks(Map<String, String> hardLinks, Path outputPath) {
        for (Map.Entry<String, String> link : hardLinks.entrySet()) {
            try {
                Path linkPath = outputPath.resolve(link.getKey());
                Path targetPath = outputPath.resolve(link.getValue());
                
                if (!Files.exists(targetPath)) {
                    System.err.println("❌ 硬链接目标不存在: " + link.getValue());
                    continue;
                }
                
                // 确保父目录存在
                Path parentDir = linkPath.getParent();
                if (parentDir != null && !Files.exists(parentDir)) {
                    Files.createDirectories(parentDir);
                }
                
                Files.createLink(linkPath, targetPath);
                System.out.println("✅ 创建硬链接: " + link.getKey() + " -> " + link.getValue());
                
            } catch (Exception e) {
                System.err.println("❌ 创建硬链接失败: " + link.getKey() + " -> " + link.getValue() + " (" + e.getMessage() + ")");
            }
        }
    }

    /**
     * 创建符号链接
     */
    private static void createSymbolicLinks(Map<String, String> symbolicLinks, Path outputPath) {
        for (Map.Entry<String, String> link : symbolicLinks.entrySet()) {
            try {
                Path linkPath = outputPath.resolve(link.getKey());
                String targetName = link.getValue();
                
                // 确保父目录存在
                Path parentDir = linkPath.getParent();
                if (parentDir != null && !Files.exists(parentDir)) {
                    Files.createDirectories(parentDir);
                }
                
                // 删除可能存在的文件
                if (Files.exists(linkPath)) {
                    Files.delete(linkPath);
                }
                
                // 创建符号链接（使用相对路径）
                Path targetPath = Paths.get(targetName);
                Files.createSymbolicLink(linkPath, targetPath);
                
                System.out.println("✅ 创建符号链接: " + link.getKey() + " -> " + targetName);
                
                // 验证符号链接
                if (Files.isSymbolicLink(linkPath)) {
                    Path actualTarget = Files.readSymbolicLink(linkPath);
                    Path resolvedTarget = linkPath.getParent().resolve(actualTarget);
                    if (Files.exists(resolvedTarget)) {
                        System.out.println("   ✅ 目标文件存在: " + resolvedTarget);
                    } else {
                        System.out.println("   ⚠️  目标文件不存在: " + resolvedTarget);
                    }
                }
                
            } catch (Exception e) {
                System.err.println("❌ 创建符号链接失败: " + link.getKey() + " -> " + link.getValue() + " (" + e.getMessage() + ")");
                
                // 尝试备用方案
                trySymlinkFallback(outputPath, link.getKey(), link.getValue());
            }
        }
    }

    /**
     * 符号链接备用方案
     */
    private static void trySymlinkFallback(Path outputPath, String linkName, String targetName) {
        try {
            Path linkPath = outputPath.resolve(linkName);
            Path targetPath = outputPath.resolve(targetName);
            
            if (Files.exists(targetPath)) {
                // 尝试硬链接
                try {
                    Files.createLink(linkPath, targetPath);
                    System.out.println("✅ 创建硬链接作为备用: " + linkName);
                    return;
                } catch (Exception e) {
                    // 硬链接失败，尝试复制
                }
                
                // 复制文件
                Files.copy(targetPath, linkPath);
                System.out.println("✅ 复制文件作为备用: " + linkName + " (大小: " + Files.size(linkPath) + " bytes)");
            } else {
                System.err.println("❌ 无法创建备用链接，目标文件不存在: " + targetName);
            }
        } catch (Exception e) {
            System.err.println("❌ 备用方案也失败: " + e.getMessage());
        }
    }

    /**
     * 设置权限和时间戳
     */
    private static void setPermissionsAndTimestamps(List<TarEntryInfo> entries, Path outputPath) {
        // 按深度排序，先处理文件，后处理目录
        entries.sort((a, b) -> {
            int depthA = a.name.split("/").length;
            int depthB = b.name.split("/").length;
            if (a.isDirectory && !b.isDirectory) return 1;
            if (!a.isDirectory && b.isDirectory) return -1;
            return Integer.compare(depthB, depthA); // 深度大的先处理
        });

        for (TarEntryInfo entryInfo : entries) {
            try {
                Path filePath = outputPath.resolve(entryInfo.name);
                if (!Files.exists(filePath)) {
                    continue;
                }

                // 设置权限
                if (PRESERVE_PERMISSIONS) {
                    setFilePermissions(filePath, entryInfo.mode, entryInfo.isDirectory);
                }

                // 设置时间戳
                if (PRESERVE_TIMESTAMPS && entryInfo.modTime != null) {
                    FileTime fileTime = FileTime.from(entryInfo.modTime.toInstant());
                    Files.setLastModifiedTime(filePath, fileTime);
                }

            } catch (Exception e) {
                System.err.println("⚠️  设置属性失败: " + entryInfo.name + " - " + e.getMessage());
            }
        }
    }

    /**
     * 设置文件权限
     */
    private static void setFilePermissions(Path filePath, int mode, boolean isDirectory) {
        try {
            // 基本权限设置
            boolean ownerRead = (mode & 0400) != 0;
            boolean ownerWrite = (mode & 0200) != 0;
            boolean ownerExecute = (mode & 0100) != 0;
            
            filePath.toFile().setReadable(ownerRead, false);
            filePath.toFile().setWritable(ownerWrite, false);
            filePath.toFile().setExecutable(ownerExecute || isDirectory, false);

            // POSIX 权限设置（如果支持）
            if (filePath.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                Set<PosixFilePermission> permissions = new HashSet<>();
                
                // 所有者权限
                if ((mode & 0400) != 0) permissions.add(PosixFilePermission.OWNER_READ);
                if ((mode & 0200) != 0) permissions.add(PosixFilePermission.OWNER_WRITE);
                if ((mode & 0100) != 0) permissions.add(PosixFilePermission.OWNER_EXECUTE);
                
                // 组权限
                if ((mode & 0040) != 0) permissions.add(PosixFilePermission.GROUP_READ);
                if ((mode & 0020) != 0) permissions.add(PosixFilePermission.GROUP_WRITE);
                if ((mode & 0010) != 0) permissions.add(PosixFilePermission.GROUP_EXECUTE);
                
                // 其他用户权限
                if ((mode & 0004) != 0) permissions.add(PosixFilePermission.OTHERS_READ);
                if ((mode & 0002) != 0) permissions.add(PosixFilePermission.OTHERS_WRITE);
                if ((mode & 0001) != 0) permissions.add(PosixFilePermission.OTHERS_EXECUTE);
                
                Files.setPosixFilePermissions(filePath, permissions);
            }
        } catch (Exception e) {
            System.err.println("⚠️  设置权限失败: " + filePath + " - " + e.getMessage());
        }
    }

    /**
     * Tar 条目信息类
     */
    private static class TarEntryInfo {
        final String name;
        final int mode;
        final boolean isDirectory;
        final Date modTime;

        TarEntryInfo(TarArchiveEntry entry) {
            this.name = entry.getName();
            this.mode = entry.getMode();
            this.isDirectory = entry.isDirectory();
            this.modTime = entry.getModTime();
        }
    }
}
