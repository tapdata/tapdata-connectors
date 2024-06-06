package io.tapdata.connector.tidb.cdc.util;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipUtils {
    public static final int COMMON_ILLEGAL_PARAMETERS = 10000;
    public static final int CLI_UNZIP_DIR_IS_FILE = 30011;

    public static void unzip(String zipFile, String outputPath) {
        if (zipFile == null || outputPath == null)
            throw new CoreException(COMMON_ILLEGAL_PARAMETERS, "Unzip missing zipFile or outputPath");
        File outputDir = new File(outputPath);
        if (outputDir.isFile())
            throw new CoreException(CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none");
        if (zipFile.endsWith(".tar.gz") || zipFile.endsWith(".gz")) {
            unTarZip(zipFile, outputPath);
        } else {
            unzip(zipFile, outputDir);
        }
    }

    public static void unTarZip(String tarFilePath, String targetDirectoryPath) {
        try (InputStream inputStream = new FileInputStream(tarFilePath)) {
            TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(inputStream);
            TarArchiveEntry entry;
            while ((entry = tarArchiveInputStream.getNextTarEntry()) != null) {
                File outputFile = new File(targetDirectoryPath, entry.getName());
                if (entry.isDirectory()) {
                    if (!outputFile.exists()) {
                        outputFile.mkdirs();
                    }
                    continue;
                }
                outputFile.getParentFile().mkdirs();
                try (OutputStream outputStream = new FileOutputStream(outputFile)) {
                    byte[] buffer = new byte[4096];
                    int len;
                    while ((len = tarArchiveInputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, len);
                    }
                }
            }
            tarArchiveInputStream.close();
        } catch (Exception e) {
            throw new CoreException(CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none, " + e.getMessage());
        }
    }

    public static void unzip(String zipFile, File outputDir) {
        if (zipFile == null || outputDir == null)
            throw new CoreException(COMMON_ILLEGAL_PARAMETERS, "Unzip missing zipFile or outputPath");
        if (outputDir.isFile())
            throw new CoreException(CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none");

        try (ZipFile zf = new ZipFile(zipFile)) {

            if (!outputDir.exists())
                FileUtils.forceMkdir(outputDir);

            Enumeration<? extends ZipEntry> zipEntries = zf.entries();
            while (zipEntries.hasMoreElements()) {
                ZipEntry entry = zipEntries.nextElement();

                try {
                    if (entry.isDirectory()) {
                        String entryPath = FilenameUtils.concat(outputDir.getAbsolutePath(), entry.getName());
                        FileUtils.forceMkdir(new File(entryPath));
                    } else {
                        String entryPath = FilenameUtils.concat(outputDir.getAbsolutePath(), entry.getName());
                        try (OutputStream fos = FileUtils.openOutputStream(new File(entryPath))) {
                            IOUtils.copyLarge(zf.getInputStream(entry), fos);
                        }
                    }
                } catch (IOException ei) {
                    ei.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteFile(String path, Log log) {
        Path directory = Paths.get(path);
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.debug("File remove failed, message: {}", e.getMessage(), e);
        }
    }
}
