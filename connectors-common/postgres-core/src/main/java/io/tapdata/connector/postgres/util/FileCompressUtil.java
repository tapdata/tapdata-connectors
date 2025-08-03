package io.tapdata.connector.postgres.util;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
             GzipCompressorInputStream gzipInputStream = new GzipCompressorInputStream(bufferedInputStream);
             TarArchiveInputStream tarInputStream = new TarArchiveInputStream(gzipInputStream)) {

            TarArchiveEntry entry;
            while ((entry = tarInputStream.getNextTarEntry()) != null) {
                extractTarEntry(tarInputStream, entry, outputDirectory);
            }
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
            extractFile(tarInputStream, outputFile);

            // 设置文件权限
            setFilePermissions(outputFile, entry);
        } else if (entry.isSymbolicLink()) {
            // 处理符号链接
            String linkTarget = entry.getLinkName();
            Path linkPath = Paths.get(outputFile.getAbsolutePath());
            Path targetPath = Paths.get(linkTarget);

            try {
                Files.createSymbolicLink(linkPath, targetPath);
            } catch (Exception e) {
                System.err.println("Failed to create symbolic link: " + linkPath + " -> " + targetPath + ", " + e.getMessage());
            }
        }
    }

    /**
     * 提取文件内容
     */
    private static void extractFile(TarArchiveInputStream tarInputStream, File outputFile) throws IOException {
        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream)) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = tarInputStream.read(buffer)) != -1) {
                bufferedOutputStream.write(buffer, 0, bytesRead);
            }
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
}
