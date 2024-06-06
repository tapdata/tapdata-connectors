package io.tapdata.connector.tidb.cdc.util;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ResourcesLoader {
    public static final int ZIP_NOT_EXISTS = 404;

    private ResourcesLoader() {
    }

    public static void unzipSources(String zipName, String outPath, Log log) {
        ClassLoader classLoader = ResourcesLoader.class.getClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(zipName)) {
            if (inputStream == null) {
                throw new CoreException(ZIP_NOT_EXISTS, "Can not find cdc tool resources, name: {}", zipName);
            }
            Path outputDir = Paths.get(outPath);
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
            }
            try (ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
                ZipEntry entry;
                while ((entry = zipInputStream.getNextEntry()) != null) {
                    Path outputFilePath = outputDir.resolve(entry.getName());
                    if (entry.isDirectory()) {
                        if (!Files.exists(outputFilePath)) {
                            Files.createDirectories(outputFilePath);
                        }
                    } else {
                        Path parentDir = outputFilePath.getParent();
                        if (!Files.exists(parentDir)) {
                            Files.createDirectories(parentDir);
                        }
                        try (FileOutputStream outputStream = new FileOutputStream(outputFilePath.toFile())) {
                            byte[] buffer = new byte[1024];
                            int length;
                            while ((length = zipInputStream.read(buffer)) > 0) {
                                outputStream.write(buffer, 0, length);
                            }
                        }
                    }
                    zipInputStream.closeEntry();
                }
            }
        } catch (IOException e) {
            log.warn("Failed to load cdc tool resources, resources name: {}, output path: {}, message: {}", zipName, outPath, e.getMessage(), e);
        }
    }
}
