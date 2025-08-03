package io.tapdata.connector.postgres.util;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

/**
 * 改进的文件压缩工具类，专门处理 tar.gz 文件的解压缩
 * 解决了原有实现中的权限和路径问题
 */
public class FileCompressUtil {

    /**
     * 从输入流中提取 tar.gz 文件到指定目录
     *
     * @param inputStream tar.gz 文件的输入流
     * @param outputDir   输出目录
     * @throws IOException 如果解压过程中发生错误
     */
    public static void extractTarGz(InputStream inputStream, String outputDir) throws IOException {
        File outputDirectory = new File(outputDir);
        if (!outputDirectory.exists() && !outputDirectory.mkdirs()) {
            throw new IOException("Failed to create output directory: " + outputDir);
        }

        System.out.println("Starting extraction to: " + outputDir);
        int fileCount = 0;
        int symlinkCount = 0;
        long totalSize = 0;

        // 存储符号链接信息，稍后处理
        java.util.List<SymlinkInfo> pendingSymlinks = new java.util.ArrayList<>();

        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
             GzipCompressorInputStream gzipInputStream = new GzipCompressorInputStream(bufferedInputStream);
             TarArchiveInputStream tarInputStream = new TarArchiveInputStream(gzipInputStream)) {

            TarArchiveEntry entry;
            while ((entry = tarInputStream.getNextTarEntry()) != null) {
                if (entry.isSymbolicLink()) {
                    // 先收集符号链接信息，稍后处理
                    pendingSymlinks.add(new SymlinkInfo(entry.getName(), entry.getLinkName(), outputDirectory));
                    symlinkCount++;
                    System.out.println("Collected symbolic link: " + entry.getName() + " -> " + entry.getLinkName());
                } else {
                    extractTarEntry(tarInputStream, entry, outputDirectory);
                    if (entry.isFile()) {
                        fileCount++;
                        totalSize += entry.getSize();
                    }
                }
            }
        }

        // 处理所有符号链接
        System.out.println("Processing " + pendingSymlinks.size() + " symbolic links...");
        for (SymlinkInfo symlinkInfo : pendingSymlinks) {
            createSymbolicLink(symlinkInfo);
        }

        System.out.println("Extraction completed. Files: " + fileCount + ", Symlinks: " + symlinkCount + ", Total size: " + totalSize + " bytes");
    }

    /**
     * 符号链接信息类
     */
    private static class SymlinkInfo {
        final String linkName;
        final String targetName;
        final File baseDirectory;

        SymlinkInfo(String linkName, String targetName, File baseDirectory) {
            this.linkName = linkName;
            this.targetName = targetName;
            this.baseDirectory = baseDirectory;
        }
    }

    /**
     * 提取单个 tar 条目
     */
    private static void extractTarEntry(TarArchiveInputStream tarInputStream, TarArchiveEntry entry, File outputDirectory) throws IOException {
        String entryName = entry.getName();

        // 防止路径遍历攻击
        if (entryName.contains("..")) {
            throw new IOException("Entry with path traversal detected: " + entryName);
        }

        File outputFile = new File(outputDirectory, entryName);

        // 确保父目录存在
        File parentDir = outputFile.getParentFile();
        if (parentDir != null && !parentDir.exists() && !parentDir.mkdirs()) {
            throw new IOException("Failed to create parent directory: " + parentDir.getAbsolutePath());
        }

        if (entry.isDirectory()) {
            // 创建目录
            if (!outputFile.exists() && !outputFile.mkdirs()) {
                throw new IOException("Failed to create directory: " + outputFile.getAbsolutePath());
            }
        } else if (entry.isFile()) {
            // 提取文件
            System.out.println("Extracting file: " + entryName + " (size: " + entry.getSize() + " bytes)");
            extractFile(tarInputStream, outputFile, entry);

            // 验证文件大小
            long actualSize = outputFile.length();
            if (actualSize != entry.getSize()) {
                throw new IOException("File size verification failed for " + entryName +
                                    ". Expected: " + entry.getSize() + ", Actual: " + actualSize);
            }

            // 设置文件权限
            setFilePermissions(outputFile, entry);
            System.out.println("Successfully extracted: " + entryName);
        } else if (entry.isSymbolicLink()) {
            // 符号链接在主循环中已经被收集，这里不应该到达
            System.err.println("Warning: Symbolic link processed in main loop: " + entryName);
        }
    }

    /**
     * 提取文件内容 - 修复版本，正确处理文件大小
     */
    private static void extractFile(TarArchiveInputStream tarInputStream, File outputFile, TarArchiveEntry entry) throws IOException {
        long fileSize = entry.getSize();

        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream)) {

            byte[] buffer = new byte[8192];
            long totalBytesRead = 0;

            while (totalBytesRead < fileSize) {
                long remainingBytes = fileSize - totalBytesRead;
                int bytesToRead = (int) Math.min(buffer.length, remainingBytes);

                int bytesRead = tarInputStream.read(buffer, 0, bytesToRead);
                if (bytesRead == -1) {
                    throw new IOException("Unexpected end of stream while extracting file: " + outputFile.getName() +
                                        ". Expected " + fileSize + " bytes, but only read " + totalBytesRead + " bytes.");
                }

                bufferedOutputStream.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
            }

            // 确保数据被写入磁盘
            bufferedOutputStream.flush();
            fileOutputStream.getFD().sync();
        }

        // 验证文件大小
        if (outputFile.length() != fileSize) {
            throw new IOException("File size mismatch for " + outputFile.getName() +
                                ". Expected: " + fileSize + " bytes, Actual: " + outputFile.length() + " bytes");
        }
    }

    /**
     * 设置文件权限
     */
    private static void setFilePermissions(File file, TarArchiveEntry entry) {
        try {
            int mode = entry.getMode();

            // 设置基本的读写执行权限
            file.setReadable((mode & 0400) != 0, false);  // 所有者读权限
            file.setWritable((mode & 0200) != 0, false);  // 所有者写权限
            file.setExecutable((mode & 0100) != 0, false); // 所有者执行权限

            // 如果支持 POSIX 权限，设置更详细的权限
            if (file.toPath().getFileSystem().supportedFileAttributeViews().contains("posix")) {
                setPosixPermissions(file, mode);
            }
        } catch (Exception e) {
            System.err.println("Failed to set permissions for file: " + file.getAbsolutePath() + ", " + e.getMessage());
        }
    }

    /**
     * 设置 POSIX 权限
     */
    private static void setPosixPermissions(File file, int mode) {
        try {
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

            Files.setPosixFilePermissions(file.toPath(), permissions);
        } catch (Exception e) {
            System.err.println("Failed to set POSIX permissions for file: " + file.getAbsolutePath() + ", " + e.getMessage());
        }
    }

    /**
     * 验证解压后的文件是否可用
     *
     * @param extractedDir  解压后的目录
     * @param expectedFiles 期望存在的文件列表
     * @return 验证结果
     */
    public static boolean validateExtractedFiles(String extractedDir, String... expectedFiles) {
        File dir = new File(extractedDir);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Extracted directory does not exist: " + extractedDir);
            return false;
        }

        for (String expectedFile : expectedFiles) {
            File file = new File(dir, expectedFile);
            if (!file.exists()) {
                System.err.println("Expected file does not exist: " + file.getAbsolutePath());
                return false;
            }

            if (expectedFile.contains("bin/") && !file.canExecute()) {
                System.err.println("Binary file is not executable: " + file.getAbsolutePath());
                return false;
            }
        }

        return true;
    }

    /**
     * 递归设置目录下所有文件的权限
     *
     * @param directory      目录
     * @param makeExecutable 是否设置为可执行
     */
    public static void setDirectoryPermissions(File directory, boolean makeExecutable) {
        if (!directory.exists() || !directory.isDirectory()) {
            return;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    setDirectoryPermissions(file, makeExecutable);
                } else {
                    file.setReadable(true, false);
                    if (makeExecutable || file.getName().endsWith(".so") ||
                            file.getParent().endsWith("bin")) {
                        file.setExecutable(true, false);
                    }
                }
            }
        }
    }

    /**
     * 验证解压后的关键文件
     */
    public static boolean validateWalMinerFiles(String walMinerDir) {
        File dir = new File(walMinerDir);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("WalMiner directory does not exist: " + walMinerDir);
            return false;
        }

        // 检查关键文件和符号链接
        return validateBinaryFiles(dir) && validateLibraryFiles(dir);
    }

    /**
     * 验证二进制文件
     */
    private static boolean validateBinaryFiles(File walMinerDir) {
        File binDir = new File(walMinerDir, "bin");
        if (!binDir.exists() || !binDir.isDirectory()) {
            System.err.println("bin directory missing: " + binDir.getAbsolutePath());
            return false;
        }

        File walMinerBin = new File(binDir, "walminer");
        if (!walMinerBin.exists()) {
            System.err.println("walminer binary missing: " + walMinerBin.getAbsolutePath());
            return false;
        }

        if (!walMinerBin.canExecute()) {
            System.err.println("walminer binary is not executable: " + walMinerBin.getAbsolutePath());
            return false;
        }

        if (!isValidELFFile(walMinerBin)) {
            System.err.println("walminer binary is not a valid ELF file: " + walMinerBin.getAbsolutePath());
            return false;
        }

        System.out.println("✅ Binary validated: " + walMinerBin.getAbsolutePath() + " (size: " + walMinerBin.length() + " bytes)");
        return true;
    }

    /**
     * 验证库文件和符号链接
     */
    private static boolean validateLibraryFiles(File walMinerDir) {
        File libDir = new File(walMinerDir, "lib");
        if (!libDir.exists() || !libDir.isDirectory()) {
            System.err.println("lib directory missing: " + libDir.getAbsolutePath());
            return false;
        }

        // 检查实际的库文件
        File libpqActual = new File(libDir, "libpq.so.5.15");
        if (!libpqActual.exists()) {
            System.err.println("Actual libpq library missing: " + libpqActual.getAbsolutePath());
            return false;
        }

        if (libpqActual.length() == 0) {
            System.err.println("Actual libpq library is empty: " + libpqActual.getAbsolutePath());
            return false;
        }

        if (!isValidELFFile(libpqActual)) {
            System.err.println("Actual libpq library is not a valid ELF file: " + libpqActual.getAbsolutePath());
            return false;
        }

        System.out.println("✅ Actual library validated: " + libpqActual.getAbsolutePath() + " (size: " + libpqActual.length() + " bytes)");

        // 检查符号链接
        File libpqSo5 = new File(libDir, "libpq.so.5");
        File libpqSo = new File(libDir, "libpq.so");

        if (!validateSymbolicLink(libpqSo5, "libpq.so.5.15")) {
            return false;
        }

        if (!validateSymbolicLink(libpqSo, "libpq.so.5.15")) {
            return false;
        }

        return true;
    }

    /**
     * 验证符号链接
     */
    private static boolean validateSymbolicLink(File linkFile, String expectedTarget) {
        if (!linkFile.exists()) {
            System.err.println("Symbolic link missing: " + linkFile.getAbsolutePath());
            return false;
        }

        try {
            if (Files.isSymbolicLink(linkFile.toPath())) {
                Path target = Files.readSymbolicLink(linkFile.toPath());
                String targetName = target.getFileName().toString();

                if (expectedTarget.equals(targetName)) {
                    System.out.println("✅ Symbolic link validated: " + linkFile.getName() + " -> " + targetName);
                    return true;
                } else {
                    System.err.println("❌ Symbolic link target mismatch: " + linkFile.getName() +
                                     " -> " + targetName + " (expected: " + expectedTarget + ")");
                    return false;
                }
            } else {
                // 不是符号链接，检查是否是有效的文件
                if (linkFile.length() > 0 && isValidELFFile(linkFile)) {
                    System.out.println("⚠️  File instead of symbolic link (but valid): " + linkFile.getName());
                    return true;
                } else {
                    System.err.println("❌ Expected symbolic link but found invalid file: " + linkFile.getAbsolutePath() +
                                     " (size: " + linkFile.length() + ")");
                    return false;
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Error checking symbolic link: " + linkFile.getAbsolutePath() + " - " + e.getMessage());
            return false;
        }
    }

    /**
     * 检查文件是否为有效的 ELF 文件（Linux 二进制文件）
     */
    public static boolean isValidELFFile(File file) {
        if (!file.exists() || file.length() < 4) {
            return false;
        }

        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] header = new byte[4];
            if (fis.read(header) != 4) {
                return false;
            }

            // ELF 文件的魔数：0x7F 'E' 'L' 'F'
            return header[0] == 0x7F && header[1] == 'E' && header[2] == 'L' && header[3] == 'F';
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 创建符号链接
     */
    private static void createSymbolicLink(SymlinkInfo symlinkInfo) {
        try {
            File linkFile = new File(symlinkInfo.baseDirectory, symlinkInfo.linkName);
            String linkTarget = symlinkInfo.targetName;

            // 确保父目录存在
            File parentDir = linkFile.getParentFile();
            if (parentDir != null && !parentDir.exists() && !parentDir.mkdirs()) {
                throw new IOException("Failed to create parent directory: " + parentDir.getAbsolutePath());
            }

            Path linkPath = linkFile.toPath();
            Path targetPath = Paths.get(linkTarget); // 使用相对路径

            // 删除可能已存在的文件
            if (Files.exists(linkPath)) {
                if (Files.isSymbolicLink(linkPath)) {
                    Path currentTarget = Files.readSymbolicLink(linkPath);
                    if (linkTarget.equals(currentTarget.toString())) {
                        System.out.println("✅ Symbolic link already correct: " + symlinkInfo.linkName + " -> " + linkTarget);
                        return;
                    }
                }
                Files.delete(linkPath);
                System.out.println("🗑️  Deleted existing file: " + symlinkInfo.linkName);
            }

            // 创建符号链接
            Files.createSymbolicLink(linkPath, targetPath);
            System.out.println("✅ Created symbolic link: " + symlinkInfo.linkName + " -> " + linkTarget);

            // 验证符号链接
            if (Files.isSymbolicLink(linkPath)) {
                Path actualTarget = Files.readSymbolicLink(linkPath);
                System.out.println("   Verified target: " + actualTarget);

                // 检查目标文件是否存在
                Path resolvedTarget = linkPath.getParent().resolve(actualTarget);
                if (Files.exists(resolvedTarget)) {
                    System.out.println("   ✅ Target file exists: " + resolvedTarget);
                } else {
                    System.out.println("   ⚠️  Target file not found: " + resolvedTarget);
                }
            }

        } catch (Exception e) {
            System.err.println("❌ Failed to create symbolic link: " + symlinkInfo.linkName + " -> " + symlinkInfo.targetName);
            System.err.println("   Error: " + e.getMessage());

            // 尝试备用方案
            createFallbackLink(new File(symlinkInfo.baseDirectory, symlinkInfo.linkName),
                             symlinkInfo.targetName, symlinkInfo.baseDirectory);
        }
    }

    /**
     * 创建备用链接（当符号链接创建失败时）
     */
    private static void createFallbackLink(File linkFile, String linkTarget, File baseDirectory) {
        try {
            // 尝试找到目标文件
            File targetFile = new File(linkFile.getParentFile(), linkTarget);
            if (!targetFile.exists()) {
                // 如果相对路径找不到，尝试在基础目录中查找
                targetFile = findFileInDirectory(baseDirectory, linkTarget);
            }

            if (targetFile != null && targetFile.exists()) {
                // 尝试创建硬链接
                try {
                    Files.createLink(linkFile.toPath(), targetFile.toPath());
                    System.out.println("Created hard link as fallback: " + linkFile.getName() + " -> " + targetFile.getName());
                } catch (Exception e) {
                    // 如果硬链接也失败，复制文件
                    Files.copy(targetFile.toPath(), linkFile.toPath());
                    System.out.println("Copied file as fallback: " + linkFile.getName() + " -> " + targetFile.getName());
                }
            } else {
                System.err.println("Cannot find target file for link: " + linkTarget);
            }
        } catch (Exception e) {
            System.err.println("Failed to create fallback link: " + e.getMessage());
        }
    }

    /**
     * 在目录中递归查找文件
     */
    private static File findFileInDirectory(File directory, String fileName) {
        if (directory == null || !directory.isDirectory()) {
            return null;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().equals(fileName)) {
                    return file;
                }
                if (file.isDirectory()) {
                    File found = findFileInDirectory(file, fileName);
                    if (found != null) {
                        return found;
                    }
                }
            }
        }
        return null;
    }
}
